defmodule ConsumerTest do
  use ExUnit.Case

  test "connecting" do
    assert {:ok, _consumer_pid } = Kafka.ConsumerSupervisor.connect('localhost', 2181)
  end
end
