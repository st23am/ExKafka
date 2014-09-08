defmodule Kafka.ConsumerSupervisor do
  use Supervisor

  @host Application.get_env(:consumer, :host)
  @port Application.get_env(:consumer, :port)

  def init(arg) do
    children = [
      worker(Kafka.Consumer, [], restart: :temporary)
    ]
    supervise(children, strategy: :simple_one_for_one)
  end

  def start_link() do
    Supervisor.start_link(__MODULE__, :ok, name: ConsumerSupervisor)
  end

  def connect do
    Supervisor.start_child(ConsumerSupervisor, [@host, @port])
  end
end
