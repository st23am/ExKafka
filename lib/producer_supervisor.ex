defmodule Kafka.ProducerSupervisor do
  use Supervisor
  @host Application.get_env(:producer, :host)
  @port Application.get_env(:producer, :port)

  def init(arg) do
    children = [
      worker(Kafka.Producer, [], restart: :temporary)
    ]

    supervise(children, strategy: :simple_one_for_one)
  end

  def start_link() do
    Supervisor.start_link(__MODULE__, :ok, name: ProducerSupervisor)
  end

  def connect do
    Supervisor.start_child(ProducerSupervisor, [@host, @port])
  end
end

