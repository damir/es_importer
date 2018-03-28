RSpec.describe EsImporter do

  es_endpoint = 'http://localhost:9200'

  $test_index = :es_importer_test_index
  client      = EsImporter.configure(es_endpoint) # it returns es client
  importer    = {
    $test_index => {
      id_key: [:user_id, :created_at],
      mapping: {user_id: :text, active: :boolean, email: :text, created_at: :date, country_code: :text},
      keywords: [:country_code],
      converters: {
        'email' => Proc.new{|attr| attr.downcase},          # existing key
        'friends.US' => Proc.new{|attr| attr << 'marry'},   # existing nested key
        'emails' => Proc.new{|doc| [doc['email']]} ,        # new key
        'profile.emails' => Proc.new{|doc| [doc['email']]}  # new nested key
      }
    }
  }

  it "adds importer" do
    EsImporter.add_importer(importer)
    expect(EsImporter.instance_eval{@importers.keys}).to eq([$test_index])
  end

  it "create es index" do
    resp = EsImporter.create_index!($test_index)
    expect(resp['acknowledged']).to eq(true)
    EsImporter.delete_index!($test_index)
  end

  it "transforms and inserts document(s)" do
    users = (1..100).to_a.map do |i|
      { user_id: i,
        created_at: Time.now.iso8601,
        active: true,
        email: 'USER_1@example.com',
        country_code: 'US',
        friends: {
          US: ['joe']
        }
      }
    end

    EsImporter.create_index!($test_index)
    10.times{EsImporter.import($test_index, users)}
    EsImporter.delete_index!($test_index)
  end
end
