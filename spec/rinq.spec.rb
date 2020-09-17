require_relative './spec_helper'
require_relative '../rinq.rb'

describe 'rinq' do
    let(:list) { [[1,2,3], 
                  [4,5,6], 
                  [7,8,9]] }

    it 'should select from a list' do
        l = list
        # instance eval keeps list from being in scope, however
        #  local function scope seems to keep closures, so w/e
        query = Rinq.query do #|some_variable|
            select :*, from(l)
        end
        result = query.exec()
        star = result.shift()
        puts star
        expect(star).to eq ['*']
        expect(result).to eq list
    end

    it 'should have a synonym for rinqydink :3' do
        l = list
        result = Rinq.ydink do
            select :*, from(l)
        end.exec
        star = result.shift
        expect(star).to eq ['*']
        expect(result).to eq list
    end

    it 'should select count from a list' do
        l = list
        result = Rinq.query do
            select count(:*), from(l)
        end.exec
        star = result.shift
        expect(star).to eq ['count(*)']
        expect(result).to eq [[3], [3], [3]]
    end

    it 'should select column values from a list' do
        l = list
        result = Rinq.query do
            select :x, :y, :z, 
            from(with_columns(l, :x, :y, :z))
        end.exec
        x, y, z = result.shift
        expect(x).to eq 'x'
        expect(y).to eq 'y'
        expect(z).to eq 'z'
        expect(result).to eq list
    end

    it 'should group_by, and join on values' do
        l = list
        other_list = list
        result = Rinq.query do
            select c(:x), :y, :z,
            from(with_columns(l, :x, :y, :z)),
            join(with_columns(other_list, :x, :y, ;z), as 'c', on { c(:x) == :x }),
            group_by :x
        end.exec
        x, y, z = result.shift
        expect(result).to eq list
    end

    it 'should select count along with columns' do
        l = list
        result = Rinq.query do
            select count(:*), :x, :y, :z,
            from(with_columns(l, :x, :y, :z))
        end.exec

        count_star, x, y, z = result.shift
        expect(count_star).to eq 'count(*)'
        expect(x).to eq 'x'
        expect(y).to eq 'y'
        expect(z).to eq 'z'

        list.each { |l| l.unshift 3 }
        expect(result).to eq list
    end

    it 'should filter results with the where clause' do
        l = list

        query = Rinq.query do #|some_variable|
            select :x, :y, :z, from(l), where { x > 1 && z < 9 }
        end
        result = query.exec()
        expect(result[1]).to eq [4,5,6]
    end

    it 'should select literal values' do
        l = list
        query = Rinq.query do
            select 1, :*, from(l)
        end

        result = query.exec()
        expect(result[0]).to eq ['1', '*']
        expect(result[1]).to eq [1, 1, 2, 3]
    end

    let(:fizzbuzz) {
        Rinq.query do
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
    }

    it 'should select proc expressions' do
        expect(fizzbuzz.exec.join(', ')).to eq 'lambda, 1, 2, 3Fizz, 4, 5Buzz'
    end

    it 'should use another datasource' do
        other_list = [*(10..15)]

        expect(fizzbuzz.exec(new_data: other_list).join(', ')).to eq 'lambda, 10Buzz, 11, 12Fizz, 13, 14, FizzBuzz'
    end

    it 'should alter data source with mutation' do
        other_list = [*(10..15)]
        fizzbuzz.set_data_source(other_list)

        expect(fizzbuzz.exec(new_data: other_list).join(', ')).to eq 'lambda, 10Buzz, 11, 12Fizz, 13, 14, FizzBuzz'
    end

    it 'should alter data source producing a new query with that data set' do
        other_list = [*(10..15)]
        newfizzbuzz = fizzbuzz.with_new_data(other_list)

        expect(newfizzbuzz.exec(new_data: other_list).join(', ')).to eq 'lambda, 10Buzz, 11, 12Fizz, 13, 14, FizzBuzz'
    end
end