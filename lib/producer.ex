defmodule Kafka.Producer do
  use GenServer

  def init([host, port]) do
    {:ok, _socket} = :gen_tcp.connect(host, port, [:binary, {:packet, 0}, {:active, false}])
  end

  def start_link(host, port) do
    GenServer.start_link(__MODULE__, [host, port])
  end

  def send_kafka_message(pid, message) do
    GenServer.call(pid, {:send_kafka_message, message})
  end

  @spec metadata(pid, integer, binary, list(binary)) :: tuple(list(tuple)) 
  def metadata(producer_pid, correlation_id, client_id, topics) do
    message = KafkaProtocol.metadata_request(correlation_id, client_id, topics)
    { :ok, response_data } = send_kafka_message(producer_pid, message)
    { _, [ {correlation_id, _, response } ] } = KafkaProtocol.parse_response_stream(response_data)
    response
  end

  def handle_call({:send_kafka_message, message}, _from, socket) do
    :ok = :gen_tcp.send(socket, message)
    {:ok, data} = :gen_tcp.recv(socket, 0)
    {:reply, {:ok, data}, socket}
  end
end
