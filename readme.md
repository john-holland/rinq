# Rinq (ruby integrated query)

Rinq is a "LINQ-like" for ruby! The primary differences being that as ruby has native functional methods for most collection types, we are just implementing the query syntax.

```ruby
query = Rinq.query do
    select :x, :y, :z, from(l), where { x > 1 && z < 9 }
end
result = query.exec()
expect(result[1]).to eq [4,5,6]
``` 

This library is a fun if not very meaningful excuse to play with domain specific languages & the method_missing method in ruby. 

As I built this as a learning toy, I'm choosing not encapsulate it in a gem.

## Features

```ruby
l = [[1,2,3],
     [4,5,6],
     [7,8,9]]
```

### `select :*, from(l)`

```ruby
result = Rinq.ydink do
    select :*, from(l)
end.exec
star = result.shift
expect(star).to eq ['*']
expect(result).to eq list
```

### `select 1, :*, from(l)`

```ruby
result = query.exec()
expect(result[0]).to eq ['1', '*']
expect(result[1]).to eq [1, 1, 2, 3]
```

### `select lambdas`
```ruby
fizzbuzz = Rinq.query do
    select ->(row) {
        val = ''
        if (row % 3 == 0)
            val += "Fizz"
        end
        if (row % 5 == 0)
            val += "Buzz"
        end
        
        if ((row % 3 != 0) || (row % 5 != 0))
            val = row.to_s + val
        end
        val
    },
    from([*(1..5)])
end

expect(fizzbuzz.exec.join(', ')).to eq 'lambda, 1, 2, 3Fizz, 4, 5Buzz'
```

### `select :x, :y, :z` position based column names and filter with `where { filter_proc }`

```ruby
query = Rinq.query do #|some_variable|
    select :x, :y, :z, from(l), where { x > 1 && z < 9 }
end
result = query.exec()
expect(result[1]).to eq [4,5,6]
```

### 

## Gotchas

This implementation primarily deals with column data in the form of 2 dimensional arrays, not iterators (something I may do in the future).

```ruby
# closures from the current scope work, 
#  however any scope above that which query is executed loses closures due to instance_eval!
l = [[1,2,3],
     [4,5,6],
     [7,8,9]]

results = Rinq.query do
    select :*, from(l)
end.exec
puts results
```

Another gotcha is you must wrap the Rinq.query call in parenthesis when passing the query into a function:

```ruby
# throws: ArgumentError: wrong number of arguments (given 0, expected 1..3)

puts Rinq.query do
    select :x, :y, :z, from(l), where { x > 1 && z < 9 }
end.exec

#prints: x y z
         4 5 6

puts(Rinq.query do
    select :x, :y, :z, from(l), where { x > 1 && z < 9 }
end.exec)
```