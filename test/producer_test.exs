defmodule ProducerTest do
  use ExUnit.Case

  import ExKafka.Constants

  test "connecting a producer" do
    assert {:ok, _consumer_pid } = Kafka.ProducerSupervisor.connect()
  end

  test "metadata request" do
    {:ok, child_pid} = Kafka.ProducerSupervisor.connect
    response = Kafka.Producer.metadata(child_pid, startmd, "foo", ["testtopic"])
    assert {[%KafkaProtocol.Broker{host: _, node_id: 0, port: _}],
            [%KafkaProtocol.TopicMetaData{partition_meta_data_list: [%KafkaProtocol.Partition{isr: [0], leader_id: 0, partition_error_code: 0, partition_id: 0, replicas: [0]}], topic_name: "testtopic"}]}
           = response
  end
end
