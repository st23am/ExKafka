defmodule KafkaTest do
  use ExUnit.Case

  test "starting the application" do
    assert {:error, {:already_started, _pid}} = Kafka.start("", "")
  end
end
