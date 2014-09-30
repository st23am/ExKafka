defmodule Kafka.Producer do
  use GenServer

  def init([host, port]) do
    {:ok, _socket} = :gen_tcp.connect(host, port, [:binary, {:packet, 0}, {:active, false}])
  end

  def start_link(host, port) do
    GenServer.start_link(__MODULE__, [host, port])
  end

  def send_kafka_messsage(pid, message) do
    GenServer.call(pid, {:send_kafka_message, message})
  end

  def handle_call({:send_kafka_message, message}, _from, socket) do
    :ok = :gen_tcp.send(socket, message)
    {:ok, data} = :gen_tcp.recv(socket, 0)
    {:reply, {:ok, data}, socket}
  end
end
