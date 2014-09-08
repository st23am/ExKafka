defmodule Kafka.Producer do
  use GenServer

  def init(host, port) do
    {:ok, conn} = :gen_tcp.connect(host, port, [:binary, {:packet, 0}])
  end

  def start_link(host, port) do
    GenServer.start_link(__MODULE__, [host, port])
  end
end
