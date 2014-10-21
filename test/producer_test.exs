defmodule ProducerTest do
  use ExUnit.Case

  test "connecting a producer" do
    assert {:ok, _consumer_pid } = Kafka.ProducerSupervisor.connect()
  end

  test "putting something into kafka" do
    {:ok, child_pid} = Kafka.ProducerSupervisor.connect
    message = KafkaProtocol.metadata_request(1, "foo", ["TestTopic"])
    assert {:ok, data} = Kafka.Producer.send_kafka_messsage(child_pid, message)
  end

  test "getting the result from kafka" do
    {:ok, child_pid} = Kafka.ProducerSupervisor.connect
    message = KafkaProtocol.metadata_request(2, "foo",["testtopic"])
    {:ok, data} = Kafka.Producer.send_kafka_messsage(child_pid, message)
    <<response_size::size(32), corr_id::size(32), data2::binary>> = data
    rs_wo_corr_id = response_size - 4
    <<response_data :: binary-size(rs_wo_corr_id), rest :: binary>> = data2
    assert {[%KafkaProtocol.Broker{host: _, node_id: 0, port: _}],
            [%KafkaProtocol.TopicMetaData{partition_meta_data_list: [%KafkaProtocol.Partition{isr: [0], leader_id: 0, partition_error_code: 0, partition_id: 0, replicas: [0]}], topic_name: "testtopic"}]}
           = KafkaProtocol.metadata_response(response_data)
  end
end
