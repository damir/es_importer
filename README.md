# Elasticsearch importer
Transform and import JSON documents into elastic search. Configure indices and transformations with ruby hash.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'es_importer'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install es_importer

## Usage

See inline coments:

```ruby
require 'es_importer'

# configure client
EsImporter.configure('http://localhost:9200', logger: Logger.new($stdout))

# generate some users
users = (1..100).to_a.map do |i|
  { user_id: i,
    first_name: 'John',
    last_name: 'Doe',
    created_at: Time.now.iso8601,
    active: true,
    email: "USER_#{i}@example.com",
    country_code: 'US',
    friends: {
      US: ['Joe']
    }
  }
end

# define mapping and settings for index
# provide conversion procs to transform the document
importer = {

  # name of the index
  # NOTE: type will be set to singular form of index name, ie. users => user
  users: {

  # build id from single or multiple keys
  id_key: [:user_id, :created_at],

  # define index mapping
  # supply a type for a field or a hash with field definition
  mapping: {
    user_id: :integer,
    active: :boolean,
    email: {type: :text, analyzer: :my_analyzer},
    created_at: :date,
    country_code: :text
  },

  # shortcut to set keywords to fields
  # keyword generated is 'country_code.keyword'
  keywords: [:country_code],

  # index settings - add a custom analyzer with a ngram filter
  settings: {
    analysis: {
      filter: {my_filter: {type: :ngram, min_gram: 3, max_gram: 4}},
	    analyzer: {my_analyzer: {type: :custom, tokenizer: :standard, filter: [:lowercase, :my_filter]}}
	  }
  },

  converters: {
    # downcase existing field value, second argument is document being processed
    'email' => Proc.new{|attr, _| attr.downcase},

    # add new entry to array under existing nested key
    'friends.US' => Proc.new{|attr| attr << 'Marry'},

    # generate new key with composite value
    'full_name' => Proc.new{|doc| "#{doc['first_name']} #{doc['last_name']}"},

    # generate new nested key as array
    'profile.emails' => Proc.new{|doc| [doc['email']]}
    }
  }
}

# register importer
EsImporter.add_importer(importer)

# create index
EsImporter.create_index!(:users)

# import single users
EsImporter.import(:users, users[0])

# import users sequentially
EsImporter.import(:users, users)

# or import in bulk
EsImporter.import_in_bulk(:users, users)

# or just transform a document without importing it
EsImporter.transform_document(:users, users[0])

#  delete index
EsImporter.delete_index!(:users)

```

AWS elastic instance is also supported, region is extracted from url and credentials are set form ruby SDK.

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/damir/es_importer. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the EsImporter project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/damir/es_importer/blob/master/CODE_OF_CONDUCT.md).
