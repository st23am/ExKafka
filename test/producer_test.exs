defmodule ProducerTest do
  use ExUnit.Case

  test "connecting a producer" do
    assert {:ok, _consumer_pid } = Kafka.ProducerSupervisor.connect()
  end

  test "putting something into kafka" do
    {:ok, pid} = Kafka.Producer.start_link('localhost', 9092)
    assert {:ok, _data} = Kafka.Producer.send_kafka_messsage(pid, "a message")
  end
end
