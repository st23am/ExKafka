defmodule ProducerTest do
  use ExUnit.Case

  test "connecting a producer" do
    assert {:ok, _consumer_pid } = Kafka.ProducerSupervisor.connect()
  end

  test "putting something into kafka" do
    {:ok, child_pid} = Kafka.ProducerSupervisor.connect
    assert {:ok, _data} = Kafka.Producer.send_kafka_messsage(child_pid, "a message")
  end
end
