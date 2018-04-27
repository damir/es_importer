require "es_importer/version"
require 'elasticsearch'

module EsImporter

  extend self

  # configure state and return es client
  def configure(uri, logger: nil)
    @importers    = {}
    @logger       = logger
    @es_uri       = uri
    @client       = Elasticsearch::Client.new transport: transport
  end

  # init ransport
  def transport
    # parse uri for host configuration
    es_uri = URI.parse(@es_uri)
    host_config = {host: es_uri.host, port: es_uri.port, scheme: es_uri.scheme}

    # aws support
    if is_amazon_uri = es_uri.host.include?('es.amazonaws.com')
      require 'aws-sdk'
      require 'faraday_middleware/aws_signers_v4'
      aws_region = es_uri.host.split('.')[-4]
      credentials = Aws::ElasticsearchService::Client.new(region: aws_region).instance_eval{@config.credentials}
    end

    faraday_config = lambda do |faraday|
      # sign for aws
      faraday.request :aws_signers_v4,
          { credentials: credentials,
            service_name: 'es',
            region: aws_region} if is_amazon_uri
      faraday.headers['Content-Type'] = 'application/json'
      faraday.adapter :typhoeus
    end
    Elasticsearch::Transport::Transport::HTTP::Faraday.new(hosts: [host_config], &faraday_config)
  end

  # save importer
  def add_importer(importer)
    @importers.update(importer)
  end

  # create index
  def create_index!(index)
    @logger&.debug("Creating #{index} index at #{@es_uri} ...")

    mapping   = @importers.dig(index, :mapping)
    keywords  = @importers.dig(index, :keywords)
    type_name = index.to_s.chomp('s')

    body = {

      # add mapping
      mappings: {
         type_name => {
          dynamic: false,
          properties: mapping.reduce({}) do |a, (k, v)|

            # field with only type def, ie. mapping: {user_id: :text}
            if v.kind_of?(Symbol)
              field_def = {type: v}
            # field with ull def, ie. mapping: {user_id: {type: :text, analyzer: :my_analyzer}}
            elsif v.kind_of?(Hash)
              field_def = v
            end

            # optional keywords
            field_def.update(keywords&.include?(k) ? {fields: {keyword: {type: :keyword}}} : {}) if keywords

            # set field definition
            a.update(k => field_def)
          end
        }
      }
    }

    # merge settings if its set
    settings = @importers.dig(index, :settings)
    body.update(settings: settings) if settings

    # create index
    @client.indices.create index: index, body: body

    rescue => error
      @logger&.debug("Error creating #{index} index. #{error.class}: #{error.message}")
      raise
  end

  # delete index
  def delete_index!(index)
    @logger&.debug("Deleting #{index} index at #{@es_uri} ...")
    @client.indices.delete index: index
    rescue => error
      @logger&.debug("Error deleting #{index} index. #{error.class}: #{error.message}")
      raise
  end

  # transform document using converters
  def transform_document(index, document)

    # convert all keys to strings
    document =  _deep_transform_keys_in_object(document, &:to_s)

    # convert keys or add new ones
    @importers.dig(index, :converters)&.each do |keys, converter|
      keys = keys.split('.')

      # transform existing key
      if value = document.dig(*keys)
        document[keys.first] = converter.call(value, document) if keys.size == 1 # lvl 1
        keys.first(keys.size-1).reduce(document, :fetch)[keys.last] = converter.call(value, document) if keys.size > 1 # lvl > 1

      # add new key
      else
        missing_key_index = nil
        keys.each_with_index do |key, i|
          missing_key_index = i and break unless document.dig(*keys.first(i + 1))
        end

        tail_keys = keys[missing_key_index..-1]
        tail_hash = keys[0...missing_key_index].reduce(document, :fetch)

        tail_keys.each_with_index do |key, i|
          tail_hash[tail_keys[i]] = tail_keys.size == i + 1 ? converter.call(document) : {}
          tail_hash = tail_hash[tail_keys[i]]
        end
      end
    end

    # add elastic id
    id_key            = @importers.dig(index, :id_key)
    generated_id      = document[id_key.to_s] if id_key.is_a?(Symbol) # single key
    generated_id      = id_key.reduce([]){|acc, key| acc << document[key.to_s]}.join('-') if id_key.is_a?(Array) # composite key
    document['es_id'] = generated_id

    # transformed document
    document
  end

  # import documents
  def import(index, documents)

    # accept single document
    documents = [documents] if documents.is_a?(Hash)

    # import stats init
    start_time  = Time.now
    imported    = {count: 0}
    failed      = {count: 0, items: []}

    # insert into elastic
    documents.each_with_index do |document, i|

      # transform document
      transformed_document = transform_document(index, document)

      # save it into index
      begin
        @client.index index: index, type: index.to_s.chomp('s'), id: transformed_document['es_id'], body: transformed_document
        imported[:count] +=1
      rescue => e
        if @logger&.debug?
          @logger.debug(e.class)
          @logger.debug(e.message)
        end
        failed[:count] +=1
        failed[:items] << {id: transformed_document['es_id'], error: e.message}
      end
    end

    # print import statistics
    if @logger&.debug?
      @logger.debug(">>> #{index} import statistics")
      @logger.debug("Imported: #{imported}")
      @logger.debug("Failed: #{failed}")
      @logger.debug("Time spent: #{Time.now - start_time} sec")
    end

    # return stats
    {imported: imported, failed: failed}
  end

  def import_in_bulk(index, documents)
    # import stats init
    start_time  = Time.now

    # transform documents and build bulk payload
    transformed_documents_for_bulk = documents.map do |document|
      transformed_document = transform_document(index, document)
      es_id = transformed_document.delete('es_id')
      {index: { _index: index, _type: index.to_s.chomp('s'), _id: es_id, data: transformed_document}}
    end

    # import
    resp = @client.bulk body: transformed_documents_for_bulk

    # print import statistics
    if @logger&.debug?
      @logger.debug(">>> #{index} import statistics")
      @logger.debug("Time spent: #{Time.now - start_time} sec")
    end

    # return deserialized es response
    resp
  end

  # taken from https://github.com/rails/rails/blob/f213e926892020f9ab6c8974612c59e2ba959253/activesupport/lib/active_support/core_ext/hash/keys.rb#L145
  def _deep_transform_keys_in_object(object, &block)
    case object
    when Hash
      object.each_with_object({}) do |(key, value), result|
        result[yield(key)] = _deep_transform_keys_in_object(value, &block)
      end
    when Array
      object.map { |e| _deep_transform_keys_in_object(e, &block) }
    else
      object
    end
  end
end
