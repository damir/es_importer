require 'pp'
require 'logger'

RSpec.describe EsImporter do

  # set env
  es_endpoint   = 'http://localhost:9200'
  logger        = Logger.new($stdout)
  logger.level  = 1

  # init index name and client
  $index    = :es_importer_test_index
  $client   = EsImporter.configure(es_endpoint, logger: logger) # it returns es client

  # index definitions
  $my_filter    = {type: 'ngram', min_gram: '3', max_gram: '4'}
  $my_analyzer  = {type: 'custom', tokenizer: 'standard', filter: ['lowercase', 'my_filter']}

  # smaple documents
  $documents = (1..5).to_a.map do |i|
    { user_id: i,
      created_at: Time.now.iso8601,
      active: true,
      email: "USER_#{i}@example.com",
      country_code: 'US',
      friends: {
        US: ['joe']
      }
    }
  end

  importer  = {
    $index => {
      id_key: [:user_id, :created_at],
      mapping: {
        user_id: :integer,
        active: :boolean,
        email: {type: :text, analyzer: :my_analyzer},
        created_at: :date, country_code: :text
      },
      keywords: [:country_code],
      settings: {analysis: {
        filter: {my_filter: $my_filter},
        analyzer: {my_analyzer: $my_analyzer}
        }
      },
      converters: {
        'email' => Proc.new{|attr| attr.downcase},          # existing key
        'friends.US' => Proc.new{|attr| attr << 'marry'},   # existing nested key
        'emails' => Proc.new{|doc| [doc['email']]} ,        # new key
        'profile.emails' => Proc.new{|doc| [doc['email']]}  # new nested key
      }
    }
  }

  before(:each) do
    $client.indices.create index: $index rescue 'no index'
  end

  after(:each) do
    $client.indices.delete index: $index rescue 'no index'
  end

  it "adds importer" do
    EsImporter.add_importer(importer)
    expect(EsImporter.instance_eval{@importers.keys}).to eq([$index])
  end

  it "creates index" do
    $client.indices.delete index: $index
    resp = EsImporter.create_index!($index)
    expect(resp['acknowledged']).to eq(true)
    expect($client.indices.exists? index: $index).to eq(true)

    # check mapping
    mapping = $client.indices.get_mapping(index: $index)
    mapping = EsImporter._deep_transform_keys_in_object(mapping, &:to_sym)
    # pp mapping
    field = mapping.dig($index, :mappings, $index, :properties, :country_code)
    expect(field[:type]).to eq('text')
    expect(field[:fields][:keyword][:type]).to eq('keyword')

    # check settings
    settings = $client.indices.get_settings(index: $index)
    settings = EsImporter._deep_transform_keys_in_object(settings, &:to_sym)
    # pp settings
    expect(settings.dig($index, :settings, :index, :analysis, :filter, :my_filter)).to eq($my_filter)
    expect(settings.dig($index, :settings, :index, :analysis, :analyzer, :my_analyzer)).to eq($my_analyzer)
  end

  it "deletes index" do
    resp = EsImporter.delete_index!($index)
    expect(resp['acknowledged']).to eq(true)
    expect($client.indices.exists? index: $index).to eq(false)
  end

  it "generates elastic id" do
    document = EsImporter.transform_document($index, $documents[0].merge(user_id: 1, created_at: 'today'))
    expect(document['es_id']).to eq('1-today')
  end

  it "transforms document" do
    document = EsImporter.transform_document($index, $documents[0])
    expect(document['email']).to eq('user_1@example.com')               # existing key
    expect(document['friends']['US']).to eq(['joe', 'marry'])           # existing nested key
    expect(document['emails']).to eq(['user_1@example.com'])            # new key
    expect(document['profile']['emails']).to eq(['user_1@example.com']) # new nested key
  end

  it "inserts document" do
    result = EsImporter.import($index, $documents[0])
    expect(result.dig(:imported, :count)).to eq(1)
  end

  it "inserts multiple documents" do

    # import valid data and check it stats
    result = EsImporter.import($index, $documents)
    expect(result.dig(:imported, :count)).to eq(5)
    expect(result.dig(:failed, :count)).to eq(0)

    # import invalid data and check it stats
    invalid_doc = $documents[0].merge(created_at: 'invalid-date')
    result = EsImporter.import($index, [invalid_doc])
    expect(result.dig(:imported, :count)).to eq(0)
    expect(result.dig(:failed, :count)).to eq(1)
    failed_item = result.dig(:failed, :items)[0]
    expect(result.dig(:failed, :items).size).to eq(1)
    expect("#{invalid_doc[:user_id]}-#{invalid_doc[:created_at]}").to eq(failed_item[:id])

    # check es response for valid transformed documents
    sleep 2
    resp = $client.search index: $index, body: {query: {match_all: {}}, size: 5, sort: {user_id: {order: :desc}}}
    resp = EsImporter._deep_transform_keys_in_object(resp, &:to_sym)

    # pp resp
    expect(resp.dig(:hits, :total)).to eq(5)

    # check transformed/generated fields
    document = resp.dig(:hits, :hits)[0][:_source]
    expect(document[:email]).to eq('user_5@example.com')              # existing key
    expect(document[:friends][:US]).to eq(['joe', 'marry'])           # existing nested key
    expect(document[:emails]).to eq(['user_5@example.com'])           # new key
    expect(document[:profile][:emails]).to eq(['user_5@example.com']) # new nested key

    # check es response for added analyzers
    resp = $client.search index: $index,
                          body: {query: {multi_match: {
                            fields: [:email],
                            query: '@exa',
                            type: :phrase_prefix,
                            analyzer: :standard
                          }}, size: 1, sort: {user_id: {order: :asc}}}
    resp = EsImporter._deep_transform_keys_in_object(resp, &:to_sym)

    # pp resp
    expect(resp.dig(:hits, :total)).to eq(5)
  end

  it "inserts multiple documents in bulk" do
    resp = EsImporter.import_in_bulk($index, $documents)
    resp = EsImporter._deep_transform_keys_in_object(resp, &:to_sym)

    # pp resp
    expect(resp[:items].size).to eq(5)
    first_item = resp[:items][0][:index]
    expect(first_item[:_id][0...12]).to eq("1-#{Date.today.strftime('%Y-%m-%d')}") # id + date YY-MM-DD
    expect(first_item[:result]).to eq('created')
  end
end
