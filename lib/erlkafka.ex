#-------------------------------------------------------------------
# File     : erlkafka.ex
# Author   : Doug Rohrer (adapted from Milind Parikh <milindparikh@gmail.com>)
#-------------------------------------------------------------------
defmodule ErlKafka do
  @moduledoc """
    -author("Milind Parikh <milindparikh@gmail.com> [http://www.milindparikh.com]"
    """

  defmacro max_msg_size, do: 1048576
  defmacro replica_id, do: -1

  defmacro produce_offset_message, do: 0

  defmacro produce_rq_required_acks, do: 1
  defmacro produce_rq_timeout, do: 1000
  defmacro fetch_rq_max_wait_time, do: 1
  defmacro fetch_rq_min_bytes, do: 0

  defmacro earliest_offset, do: -2
  defmacro latest_offset, do: -1
  defmacro max_offsets, do: 20

  defmacro rq_produce, do: 0
  defmacro rq_fetch, do: 1
  defmacro rq_offset, do: 2
  defmacro rq_metadata, do: 3
  defmacro rq_leaderandisr, do: 4
  defmacro rq_stopreplica, do: 5
  defmacro rq_offsetcommit, do: 6
  defmacro rq_offsetfetch, do: 7

  defmacro api_v_produce, do: 0 
  defmacro api_v_fetch, do: 0
  defmacro api_v_offset, do: 0
  defmacro api_v_metadata, do: 0
  defmacro api_v_leaderandisr, do: 0
  defmacro api_v_stopreplica, do: 0
  defmacro api_v_offsetcommit, do: 0
  defmacro api_v_offsetfetch, do: 0


  defmacro compression_none, do: 0
  defmacro compression_gzip, do: 1
  defmacro compression_snappy, do: 2

  defmacro magic_byte, do: 0
  defmacro err_none, do: 0
  defmacro err_unknown, do: -1
  defmacro err_offsetoutofrange, do: 1
  defmacro err_invalidmessage, do: 2
  defmacro err_unknowntopicorpartition, do: 3
  defmacro err_invalidmessagesize, do: 4
  defmacro err_leadernotavailable, do: 5
  defmacro err_notleaderforpartition, do: 6
  defmacro err_requesttimedout, do: 7
  defmacro err_brokernotavailable, do: 8
  defmacro err_replicanotavailable, do: 9
  defmacro err_messagesizetoolarge, do: 10
  defmacro err_stalecontrollerepochcode, do: 11
  defmacro err_offsetmetadatatoolargecode, do: 12

  defmacro max_bytes_fetch, do: 10000

  defmacro startmd, do: 2000000000
  defmacro endmd, do: 2999999999
  defmacro startfe, do: 1
  defmacro endfe, do: 999999999
  defmacro startpr, do: 1000000000
  defmacro endpr, do: 1999999999
  defmacro startor, do: 3000000000
  defmacro endor, do: 3500000000
  defmacro startof, do: 3500000001
  defmacro endof, do: 3999999999
  defmacro startoc, do: 4000000000
  defmacro endoc, do: 4294967295

end
