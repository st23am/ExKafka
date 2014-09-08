defmodule ConsumerTest do
  use ExUnit.Case

  test "connecting" do
    assert {:ok, _consumer_pid } = Kafka.ConsumerSupervisor.connect()
  end
end
