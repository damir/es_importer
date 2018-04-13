require 'pp'
es_endpoint = 'http://localhost:9200'

RSpec.describe EsImporter do
  $index    = :es_importer_test_index
  $client   = EsImporter.configure(es_endpoint) # it returns es client

  # index definitions
  $my_filter    = {type: 'ngram', min_gram: '3', max_gram: '4'}
  $my_analyzer  = {type: 'custom', tokenizer: 'standard', filter: ['lowercase', 'my_filter']}

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
    pp mapping
    field = mapping.dig($index, :mappings, $index, :properties, :country_code)
    expect(field[:type]).to eq('text')
    expect(field[:fields][:keyword][:type]).to eq('keyword')

    # check settings
    settings = $client.indices.get_settings(index: $index)
    settings = EsImporter._deep_transform_keys_in_object(settings, &:to_sym)
    pp settings
    expect(settings.dig($index, :settings, :index, :analysis, :filter, :my_filter)).to eq($my_filter)
    expect(settings.dig($index, :settings, :index, :analysis, :analyzer, :my_analyzer)).to eq($my_analyzer)
  end

  it "deletes index" do
    resp = EsImporter.delete_index!($index)
    expect(resp['acknowledged']).to eq(true)
    expect($client.indices.exists? index: $index).to eq(false)
  end

  it "transforms and inserts document(s)" do
    users = (1..5).to_a.map do |i|
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

    # import data
    EsImporter.import($index, users)
    sleep 2

    # check response
    resp = $client.search index: $index, body: {query: {match_all: {}}, size: 5, sort: {user_id: {order: :desc}}}
    resp = EsImporter._deep_transform_keys_in_object(resp, &:to_sym)
    pp resp
    expect(resp.dig(:hits, :total)).to eq(5)

    # check converters
    user = resp.dig(:hits, :hits)[0][:_source]
    expect(user[:email]).to eq('user_5@example.com')              # existing key
    expect(user[:friends][:US]).to eq(['joe', 'marry'])           # existing nested key
    expect(user[:emails]).to eq(['user_5@example.com'])           # new key
    expect(user[:profile][:emails]).to eq(['user_5@example.com']) # new nested key

    # check analyzers
    resp = $client.search index: $index,
                          body: {query: {multi_match: {
                            fields: [:email],
                            query: '@exa',
                            type: :phrase_prefix,
                            analyzer: :standard
                          }}, size: 1, sort: {user_id: {order: :asc}}}
    resp = EsImporter._deep_transform_keys_in_object(resp, &:to_sym)
    pp resp
    expect(resp.dig(:hits, :total)).to eq(5)
  end
end
