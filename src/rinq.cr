module Rinq
  class Query
    getter from : From
    getter select : Select
    getter where : Where?
    getter order_by : OrderBy?
    getter limit : Int32?
    getter offset : Int32?
    getter group_by : Array(Symbol)?

    def initialize(@from, @select, @where = nil, @order_by = nil, @limit = nil, @offset = nil, @group_by = nil)
    end

    def exec(new_data = nil)
      from = new_data ? From.new(new_data) : @from
      data = @where ? @where.not_nil!.apply(self, from) : from.data_source
      
      # Apply group by if present
      if @group_by
        data = group_data(data)
      end
      
      # Apply order by if present
      if @order_by
        data = @order_by.not_nil!.apply(data)
      end
      
      # Apply limit and offset
      if @limit || @offset
        data = apply_limit_offset(data)
      end
      
      @select.apply(self, data)
    end

    private def group_data(data : Array)
      grouped = {} of Array -> Array
      data.each do |row|
        key = @group_by.not_nil!.map { |col| row[@from.index_for_column_name(col).not_nil!] }
        grouped[key] ||= [] of Array
        grouped[key] << row
      end
      grouped.values.flatten
    end

    private def apply_limit_offset(data : Array)
      start = @offset || 0
      length = @limit ? @limit.not_nil! : data.size
      data[start, length]
    end

    def with_new_data(data_source)
      Query.new(data_source ? From.new(data_source) : @from, @select, @where, @order_by, @limit, @offset, @group_by)
    end

    def set_data_source(data_source)
      @from = data_source ? From.new(data_source) : @from
    end
  end

  macro query(&block)
    {{yield}}
  end

  macro ydink(&block)
    query {{yield}}
  end

  class From
    getter data_source : Array
    getter columns : Int32
    property column_names : Array(Symbol)

    def initialize(@data_source)
      @column_names = [] of Symbol
      @columns = @data_source.reduce(0) do |acc, c|
        if c.responds_to?(:any?) && c.responds_to?(:size)
          if acc < c.size
            c.size
          else
            acc
          end
        else
          acc
        end
      end
    end

    def size
      @data_source.size
    end

    def index_for_column_name(name : Symbol) : Int32?
      @column_names.index(name)
    end
  end

  class Select
    def initialize(@queries : Array)
    end

    def apply(query : Query, result_set : Array)
      first_row = @queries.map do |q|
        case q
        when Symbol
          q.to_s
        when Proc
          :lambda
        when Count
          q.to_s
        when Aggregate
          q.to_s
        else
          q.to_s
        end
      end

      [
        first_row,
        *(0..result_set.size-1).map do |i|
          @queries.flat_map do |q|
            case q
            when :*
              result_set[i].responds_to?(:each) ? result_set[i] : [result_set[i]]
            when Count
              [q.apply(query, result_set)]
            when Aggregate
              [q.apply(query, result_set)]
            when Symbol
              si = query.from.index_for_column_name(q)
              if si && i > 0
                result_set[i][si].responds_to?(:each) ? result_set[i][si] : [result_set[i][si]]
              else
                nil
              end
            when Proc
              value = q.call(result_set[i])
              value.responds_to?(:each) ? value : [value]
            else
              [q]
            end
          end
        end
      ]
    end
  end

  class OrderBy
    getter columns : Array({Symbol, Symbol})

    def initialize(@columns)
    end

    def apply(data : Array)
      data.sort do |a, b|
        @columns.each do |col, direction|
          a_val = a[col]
          b_val = b[col]
          comp = a_val <=> b_val
          return direction == :asc ? comp : -comp if comp != 0
        end
        0
      end
    end
  end

  class Aggregate
    getter function : Symbol
    getter column : Symbol

    def initialize(@function, @column)
    end

    def apply(query : Query, results : Array)
      values = results.map { |r| r[query.from.index_for_column_name(@column).not_nil!] }
      case @function
      when :sum
        values.sum
      when :avg
        values.sum / values.size.to_f
      when :min
        values.min
      when :max
        values.max
      else
        raise "Unknown aggregate function: #{@function}"
      end
    end

    def to_s
      "#{@function}(#{@column})"
    end
  end

  def self.select(*args)
    from = nil
    where = nil
    order_by = nil
    limit = nil
    offset = nil
    group_by = nil

    case args
    when {*queries, From, Where, OrderBy, Int32, Int32, Array(Symbol)}
      from = args[-6]
      where = args[-5]
      order_by = args[-4]
      limit = args[-3]
      offset = args[-2]
      group_by = args[-1]
    when {*queries, From, Where, OrderBy, Int32, Int32}
      from = args[-5]
      where = args[-4]
      order_by = args[-3]
      limit = args[-2]
      offset = args[-1]
    when {*queries, From, Where, OrderBy}
      from = args[-3]
      where = args[-2]
      order_by = args[-1]
    when {*queries, From, Where}
      from = args[-2]
      where = args[-1]
    when {*queries, From}
      from = args[-1]
    end

    query = Query.new(from(from), Select.new(args[0..-2]), where, order_by, limit, offset, group_by)
    query.from.column_names = if query.from.is_a?(ColumnData)
      query.from.column_names
    else
      args[0..-2].map { |q| q.is_a?(Symbol) ? q : :__misc_object_ }
    end
    
    query
  end

  def self.from(data_source)
    if data_source.is_a?(From)
      return data_source
    end

    From.new(data_source)
  end

  def self.count(symbol : Symbol)
    Count.new(symbol)
  end

  def self.sum(column : Symbol)
    Aggregate.new(:sum, column)
  end

  def self.avg(column : Symbol)
    Aggregate.new(:avg, column)
  end

  def self.min(column : Symbol)
    Aggregate.new(:min, column)
  end

  def self.max(column : Symbol)
    Aggregate.new(:max, column)
  end

  def self.order_by(*columns)
    OrderBy.new(columns.map { |c| {c, :asc} })
  end

  def self.order_by_desc(*columns)
    OrderBy.new(columns.map { |c| {c, :desc} })
  end

  class ColumnData < From
    def initialize(data, @column_names : Array(Symbol))
      super(data)
    end
  end

  def self.with_columns(data_source, *names)
    ColumnData.new(data_source, names.map(&.as(Symbol)))
  end

  class Where
    def initialize(@predicate : Proc)
      @results = [] of Any
      @query = nil
    end

    def apply(query : Query, from : From)
      results = [] of Array
      @query = query
      
      from.size.times do |row|
        @results = from.data_source[row]
        if @predicate.call
          results << @results
        end
      end
      
      @results = [] of Any
      @query = nil
      results
    end

    def method_missing(name : Symbol, *args)
      return nil unless @query

      i = @query.from.index_for_column_name(name)
      return nil unless i

      @results[i]
    end
  end

  def self.where(predicate : Proc? = nil, &block : -> Bool)
    Where.new(block || predicate.not_nil!)
  end
end 