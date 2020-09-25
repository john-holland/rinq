module Rinq
    class Query
        attr_reader :from, :select, :where

        def initialize(from, select, where)
            @from = from
            @select = select
            @where = where
        end

        def exec(new_data: nil)
            from = new_data ? From.new(new_data) : @from
            @select.apply(self, @where ? 
                @where.apply(self, from)
                : from.data_source)
        end

        def with_new_data(data_source)
            Query.new(data_source ? From.new(data_source) : @from, @select, @where)
        end

        def set_data_source(data_source)
            @from = data_source ? From.new(data_source) : @from
        end
    end
 
    def self.iterator(&block)
        iterator_query = instance_eval(&block)
        iterator_query
    end
    
    def self.query(&block)
        query = instance_eval(&block)
        query
    end

    def self.ydink(&block)
        self.query(&block)
    end

    class From
        attr_reader :data_source, :columns, :column_names
        attr_writer :column_names

        def initialize(data_source)
            @data_source = data_source
            @column_names = []
            @columns = @data_source.inject(0) { |a, c|
                if (c.methods.include?(:any?) && c.methods.include?(:length))
                    if a < c.length
                        return c.length
                    end
                end

                a
            }
        end

        def length
            @data_source.length
        end

        def index_for_column_name(name)
            @column_names.find_index name
        end
    end

    class Select
        def initialize(queries)
            @queries = queries
        end

        def apply(query, result_set)
            first_row = @queries.map { |q|
                case q
                    in Symbol => s
                        s.to_s
                    in Proc => p
                        :lambda
                    in Count => c
                        c.to_s
                    else
                        q.to_s
                end
            }

            [
                first_row,
                *(0..result_set.length-1).map { |i|
                    @queries.flat_map { |q| 
                        case q
                        in :* => star
                        result_set[i].respond_to?('each') ? result_set[i] : [result_set[i]] 
                        in Count => c
                            [c.apply(query, result_set)]
                        in Symbol => s
                            si = query.from.index_for_column_name(s.to_sym)
                            
                            # todo: make sure to check if 0 is falsy (sp?)
                            !!i ? (result_set[i][si].respond_to?('each') ? result_set[i][si] : [result_set[i][si]]) : nil
                        in Proc => p
                            value = p.call(result_set[i])
                            value.respond_to?('each') ? value : [value] 
                        else
                            [q] #return whatever literal
                        end
                    }
                }
            ]
        end
    end

    def self.select(*args)
        from = nil
        where = nil
        group_by = nil
        as = nil
        join = []

        case args
            in [*queries, From => from, Join => *join]
            in [*queries, From => from, Join => *join, Where => where]
            in [*queries, From => from, Join => *join, Where => where]
            in [*queries, From => from, Where => where, GroupBy => group_by, As => as]
            in [*queries, From => from, Where => where, GroupBy => group_by]
            in [*queries, From => from, Where => where]
            in [*queries, From => from, GroupBy => group_by]
            in [*queries, From => from]
        end

        query = Query.new(from(from), Select.new(queries), where, group_by)
        query.from.column_names = if query.from.is_a? ColumnData
                                    query.from.column_names
                                  else
                                    queries.map { |q| q.is_a?(Symbol) ? q : :__misc_object_ }
                                  end
        
        query
    end

    def self.from(data_source)
        if data_source.is_a? From
            return data_source
        end

        From.new(data_source)
    end

    class Count
        def initialize(symbol)
            @symbol = symbol
        end

        def apply(query, results)
            if @symbol == :*
                results.length
            else
                i = query.index_for_column_name @symbol
                results.count { |x| !x[i].nil? }
            end
        end

        def to_s
            "count(#{@symbol})"
        end
    end

    def self.count(symbol)
        Count.new(symbol)
    end

    class ColumnData < From
        attr_accessor :column_names

        def initialize(data, column_names)
            super(data)
            @column_names = column_names
        end
    end

    def self.with_columns(data_source, *names)
        ColumnData.new(data_source, names)
    end

    class Where
        def initialize(predicate)
            @predicate = predicate
            @results = []
            @query = nil
        end

        def apply(query, from)
            results = []
            @query = query
            # puts from.data_source
            length = from.length
            (0..length-1).each { |row|
                @results = from.data_source[row]
                # consider passing the row in here?
                if self.instance_eval(&@predicate)
                    results << @results
                end
            }
            @results = []
            @query = nil
            results
        end

        def method_missing(name, *args)
            if (!@query) 
               # raise RuntimeError("the where clause was called without being passed a query")
            end

            i = @query.from.index_for_column_name(name)
            if i == -1 || i.nil?
                return nil
            end

            # puts i
            @results[i]
        end
    end

    def method_missing(name, *args)
        if (args.length == 0 || args.length > 1)
            throw new RuntimeError('table alias failed as no column specifier was referenced')
        end
        return Alias.new(name, args[0])
    end

    class Alias
        attr_reader :name, :column_reference

        def initialize(name, column_reference)
            @name = name
            @column_reference = column_reference
        end
    end

    def self.where(predicate = nil, &block_predicate)
        Where.new(block_predicate || predicate)
    end

    def self.group_by(&block_classifier)
        GroupBy.new(block_classifier)
    end

    class GroupBy
        def initialize(query, block_classifier)
            @query = query
            @block_classifier = block_classifier
        end

        def apply(data_source)
            data_source.group_by(&@block_classifier)
        end

        def method_missing(name, *args)
            if (!@query) 
                #raise RuntimeError("the where clause was called without being passed a query")
            end

            i = @query.from.index_for_column_name(name)
            if i == -1 || i.nil?
                return nil
            end

            # puts i
            @results[i]
        end
    end

    class As
        attr_reader name;

        def initialize(name, query)
            @name = name
            @query = query
        end

        def apply
            @query.column_names << @name
        end
    end
end
