defmodule Kafka.ConsumerSupervisor do
  use Supervisor

  def init(arg) do
    children = [
      worker(Kafka.Consumer, [], restart: :temporary)
    ]
    supervise(children, strategy: :simple_one_for_one)
  end

  def start_link() do
    Supervisor.start_link(__MODULE__, :ok)
  end

  def connect(host, port) do
    :gen_tcp.connect(host, port, [:binary, {:packet, 0}])
  end
end
