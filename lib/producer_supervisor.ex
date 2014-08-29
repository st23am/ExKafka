defmodule Kafka.ProducerSupervisor do
  use Supervisor

  def init(arg) do
    children = [
      worker(Kafka.Producer, [], restart: :temporary)
    ]

    supervise(children, strategy: :simple_one_for_one)
  end

  def start_link() do
    Supervisor.start_link(__MODULE__, :ok)
  end
end
