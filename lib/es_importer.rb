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

  # init es transport
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

  # create es index
  def create_index!(es_index)
    puts; puts "Creating #{es_index} index at #{@es_uri} ..."
    @client.indices.create index: es_index, body: {
      mappings: {
        es_index.to_s.chomp('s') => {
          dynamic: false,
          properties: @importers.dig(es_index, :mapping).reduce({}){|a, (k,v)| a.update({k => {type: v}.update(@importers.dig(es_index, :keywords)&.include?(k) ? {fields: {keyword: {type: :keyword}}} : {})})}
        }
      }
    }
    rescue => error
      puts "Error creating #{es_index} index. #{error.class}: #{error.message}"
      raise
  end

  # create es index
  def delete_index!(es_index)
    puts; puts "Deleting #{es_index} index at #{@es_uri} ..."
    @client.indices.delete index: $test_index
    rescue => error
      puts "Error deleting #{es_index} index. #{error.class}: #{error.message}"
      raise
  end

  # import documents
  def import(es_index, documents)

    # import stats init
    start_time = Time.now
    failed = 0; imported = 0

    # insert into elastic
    documents.each_with_index do |document, index|

      # convert all keys to strings
      document =  _deep_transform_keys_in_object(document, &:to_s)

      # generate id
      id_key  = @importers.dig(es_index, :id_key)
      id      = document[id_key.to_s] if id_key.is_a?(Symbol) # single key
      id      = id_key.reduce([]){|acc, key| acc << document[key.to_s]}.join('-') if id_key.is_a?(Array) # composite key

      # convert keys or add new ones
      @importers.dig(es_index, :converters)&.each do |keys, converter|
        keys = keys.split('.')

        # transform existing key
        if value = document.dig(*keys)
          document[keys.first] = converter.call(value, document) if keys.size == 1 # lvl 1
          keys.first(keys.size-1).reduce(document, :fetch)[keys.last] = converter.call(value, document) if keys.size > 1 # lvl > 1

        # add new key
        else
          missing_key_index = nil
          keys.each_with_index do |key, index|
            missing_key_index = index and break unless document.dig(*keys.first(index + 1))
          end

          tail_keys = keys[missing_key_index..-1]
          tail_hash = keys[0...missing_key_index].reduce(document, :fetch)

          tail_keys.each_with_index do |key, index|
            tail_hash[tail_keys[index]] = tail_keys.size == index + 1 ? converter.call(document) : {}
            tail_hash = tail_hash[tail_keys[index]]
          end
        end
      end

      begin
        @client.index index: es_index, type: es_index.to_s.chomp('s'), id: id, body: document
        puts "##{index + 1} imported #{id}" if @logger
        imported = imported + 1
      rescue => e
        puts "##{index + 1} failed #{id}" if @logger
        puts e.class; puts e.message
        failed = failed + 1
      end
    end

    # print import statistics
    puts; puts "#{es_index} import statistics"; puts '-' * 100
    puts "Failed: #{failed}"
    puts "Imported: #{imported}"
    puts "Time spent: #{Time.now - start_time} sec"
    puts
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
