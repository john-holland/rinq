require "spec"
require "../src/rinq"

describe Rinq do
  describe "basic query functionality" do
    it "performs simple select" do
      data = [
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ]
      
      result = Rinq.select(:*, Rinq.from(data))
      result.exec.should eq([
        ["*"],
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ])
    end

    it "performs select with where" do
      data = [
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ]
      
      result = Rinq.select(:*, Rinq.from(data), Rinq.where { self[0] > 1 })
      result.exec.should eq([
        ["*"],
        [2, "b"],
        [3, "c"]
      ])
    end

    it "performs select with column names" do
      data = [
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ]
      
      result = Rinq.select(:id, :name, Rinq.with_columns(data, :id, :name))
      result.exec.should eq([
        ["id", "name"],
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ])
    end

    it "performs count operation" do
      data = [
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ]
      
      result = Rinq.select(Rinq.count(:*), Rinq.from(data))
      result.exec.should eq([
        ["count(*)"],
        [3]
      ])
    end

    it "performs count with where" do
      data = [
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ]
      
      result = Rinq.select(Rinq.count(:*), Rinq.from(data), Rinq.where { self[0] > 1 })
      result.exec.should eq([
        ["count(*)"],
        [2]
      ])
    end
  end

  describe "SQL-like features" do
    it "performs order by" do
      data = [
        [3, "c"],
        [1, "a"],
        [2, "b"]
      ]
      
      result = Rinq.select(:*, Rinq.with_columns(data, :id, :name), Rinq.order_by(:id))
      result.exec.should eq([
        ["*"],
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ])
    end

    it "performs order by desc" do
      data = [
        [1, "a"],
        [3, "c"],
        [2, "b"]
      ]
      
      result = Rinq.select(:*, Rinq.with_columns(data, :id, :name), Rinq.order_by_desc(:id))
      result.exec.should eq([
        ["*"],
        [3, "c"],
        [2, "b"],
        [1, "a"]
      ])
    end

    it "performs limit and offset" do
      data = [
        [1, "a"],
        [2, "b"],
        [3, "c"],
        [4, "d"]
      ]
      
      result = Rinq.select(:*, Rinq.with_columns(data, :id, :name), Rinq.order_by(:id), 2, 1)
      result.exec.should eq([
        ["*"],
        [2, "b"],
        [3, "c"]
      ])
    end

    it "performs group by" do
      data = [
        [1, "a", 100],
        [1, "b", 200],
        [2, "c", 300],
        [2, "d", 400]
      ]
      
      result = Rinq.select(:*, Rinq.with_columns(data, :id, :name, :value), Rinq.order_by(:id), nil, nil, [:id])
      result.exec.should eq([
        ["*"],
        [1, "a", 100],
        [1, "b", 200],
        [2, "c", 300],
        [2, "d", 400]
      ])
    end

    it "performs aggregate functions" do
      data = [
        [1, 100],
        [2, 200],
        [3, 300]
      ]
      
      result = Rinq.select(
        Rinq.sum(:value),
        Rinq.avg(:value),
        Rinq.min(:value),
        Rinq.max(:value),
        Rinq.with_columns(data, :id, :value)
      )
      result.exec.should eq([
        ["sum(value)", "avg(value)", "min(value)", "max(value)"],
        [600, 200.0, 100, 300]
      ])
    end
  end

  describe "query macro" do
    it "works with query macro" do
      data = [
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ]
      
      result = Rinq.query do
        select(:*, from(data))
      end
      
      result.exec.should eq([
        ["*"],
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ])
    end

    it "works with ydink macro" do
      data = [
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ]
      
      result = Rinq.ydink do
        select(:*, from(data))
      end
      
      result.exec.should eq([
        ["*"],
        [1, "a"],
        [2, "b"],
        [3, "c"]
      ])
    end
  end
end 