##%-------------------------------------------------------------------
##% File     : kafka_protocol008000000000.erl
## Kafka Version 0.8   (this file names it 00.80.00)
## Against
##   Request            ApiVersion
##     ProduceRequest 	     0
##     FetchRequest 	     0
##     OffsetRequest 	     0
##     MetadataRequest 	     0
##     LeaderAndIsrRequest   0
##     StopReplicaRequest    0
##     OffsetCommitRequest   0
##     OffsetFetchRequest    0
##% Author   : Milind Parikh <milindparikh@gmail.com>
##%-------------------------------------------------------------------
defmodule KafkaProtocol do

  import ExKafka.Constants

  ## Initial philosophy is derived from
  ##     https://github.com/wooga/kafka-erlang.git
  ## The kafka protocol is tested against kafka 0.8 (Beta)


  defmodule Broker do
    defstruct [:node_id, :host, :port]
  end

  defmodule TopicMetaData do
    defstruct [:topic_name, :partition_meta_data_list]
  end

  defmodule Partition do
    defstruct [:partition_error_code, :partition_id, :leader_id, :replicas, :isr]
  end

  ##%-------------------------------------------------------------------
  ##%                         API FUNCTIONS
  ##%-------------------------------------------------------------------



  ##%-------------------------------------------------------------------
  ##%                         API/METADATA FUNCTION
  ##%-------------------------------------------------------------------

  ##  @doc The default metadata request
  ##           metadata_request(2100000000, "ERLKAFKA", ["testtopic1", "testtopic2"])
  ##


  ## -spec metadata_request() -> binary().

  @spec metadata_request(integer,binary,list(binary)) :: binary
  def metadata_request(correlation_id, client_id, topics) do
    common_part = common_part_of_request(rq_metadata, api_v_metadata, correlation_id, client_id)
    #:io.format("COMMON PART ~p~n", [common_part])
    topic_count = length(topics)

    var_meta_data_request =
    case topic_count do
      0 ->
        <<topic_count :: size(32), -1 :: size(16)>>
      _ ->
      topics_as_kafka_strings = List.foldl(topics, <<"">>, fn(x, a) ->
        lob = :erlang.size(x)
        <<lob :: size(16),x :: binary, a :: binary>>
      end)
      << topic_count :: size(32), topics_as_kafka_strings :: binary>>
    end
    meta_data_request = << common_part :: binary, var_meta_data_request :: binary>>
    size_meta_data_request = :erlang.size(meta_data_request)
    <<size_meta_data_request :: size(32), meta_data_request :: binary>>
  end

  def metadata_response(data) do
    { broker_list, topic_metadata_bin } = parse_brokers_info(data)
    { topic_metadata_list, _} = parse_topic_meta_data(topic_metadata_bin)
    { broker_list, topic_metadata_list }
  end

  #  ##%-------------------------------------------------------------------
  #  ##%                         API/PRODUCE FUNCTION
  #  ##%-------------------------------------------------------------------
  #
  #  ##  @doc The         PRODUCE  request
  #  ##           produce_request(0000000001, "ERLKAFKA", [
  #  ##                                                    {"testtopic1",
  #  ##                                                           [
  #  ##							      {0, [{"MSG1"}, {"MSG2"}]},
  #  ##							      {1, [{"MSG3"}, {"MSG4"}]}
  #  ##                                                           ]
  #  ##						      },
  #  ##                                                    {"testtopic2",
  #  ##                                                           [
  #  ##							      {0, [{"MSG5"}, {"com.amazon", "MSG6"}]},
  #  ##							      {1, [{"MSG7"}, {"MSG8"}]}
  #  ##                                                           ]
  #  ##						      }
  #  ##						     ]
  #  ##			     )
  #
  #
  #
  #  % -spec        produce_request(
  #  %		       CorrelationId::integer(),
  #  %                       ClientId::list(),
  #  %                       Topics::list()) ->
  #  %			      binary().
  #
  #
  #
  #  produce_request ( CorrelationId,
  #                    ClientId,
  #                    Topics
  #                  ) ->
  #
  #      produce_request ( CorrelationId,
  #                        ClientId,
  #                        Topics,
  #                        ?PRODUCE_RQ_REQUIRED_ACKS,
  #                        ?PRODUCE_RQ_TIMEOUT).
  #
  #  %-spec produce_request(
  #  %	CorrelationId::integer(),
  #  %	ClientId::list(),
  #  %	Topics::list(),
  #  %	RequiredAcks:integer(),
  #  %	Timeout:integer()
  #  %       ) ->
  #  %			     binary().
  #
  #
  #  produce_request ( CorrelationId,
  #                    ClientId,
  #                    Topics,
  #                    RequiredAcks,
  #                    TimeOut
  #                  ) ->
  #
  #      CommonPartOfRequest = common_part_of_request(ExKafka.Constants.RQ_PRODUCE,
  #                                                    ?API_V_PRODUCE,
  #                                                    CorrelationId,
  #                                                    ClientId),
  #
  #      VarProduceRequest = var_part_of_produce_request(Topics),
  #
  #      ProduceRequest =
  #          <<CommonPartOfRequest/binary,
  #            RequiredAcks:16/integer,
  #            TimeOut:32/integer,
  #            VarProduceRequest/binary>>,
  #
  #      ProduceRequestSize = size(ProduceRequest),
  #
  #      <<ProduceRequestSize:32/integer, ProduceRequest/binary>>.
  #
  #
  #
  #
  #
  #  %-spec produce_response(
  #  %		       Data::binary()) ->
  #  %			      {TopicList}
  #
  #
  #  produce_response(Data) ->
  #      Topics = parse_topics_for_produce_request(Data),
  #      {Topics}.
  #
  #
  #
  #
  #
  #  ##%-------------------------------------------------------------------
  #  ##%             Fetch Request
  #  ##%-------------------------------------------------------------------
  #  ## @doc Generate fetch request - TopicList has list of topics
  #  ##      where each element is tuple containing topic name and
  #  ##      list of partition along with first off set to be read, sample
  #  ##      TopicList = [{"topic1", [{0, 0}, {1,0}, {2,0}]},
  #  ##				 {"topic2", [{0, 0}, {1,0}, {2,0}]}].
  #
  #  -spec fetch_request(CorrelationId::integer(), ClientId::list(), TopicList::list()) -> binary.
  #
  #  fetch_request(CorrelationId, ClientId, TopicList) ->
  #          fetch_request(CorrelationId, ClientId, ?FETCH_RQ_MAX_WAIT_TIME, ?FETCH_RQ_MIN_BYTES, TopicList).
  #
  #  -spec fetch_request(CorrelationId::integer(), ClientId::list(), MaxWaitTime::integer(), MinBytes::integer(), TopicList::list()) -> binary.
  #
  #  fetch_request(CorrelationId, ClientId, MaxWaitTime, MinBytes, TopicList) ->
  #          ## @TODO: Make partitions optional
  #          FetchTopicList = lists:foldl(fun(X, List) ->
  #                {TopicName, PartitionData} = X,
  #                Partitions = lists:foldl(fun (Y, PartitionList) ->
  #                                                                    {Partition, Offset} = Y,
  #                                                                    [<<Partition:32/integer, Offset:64/integer, ?MAX_BYTES_FETCH:32/integer>> | PartitionList]
  #                                                                    end, [], PartitionData),
  #                            PartitionBin = create_array_binary(Partitions),
  #                            TopicNameBin = create_string_binary(TopicName),
  #                            [<<TopicNameBin/binary, PartitionBin/binary>> | List]
  #                           end, [], TopicList),
  #          FetchTopicBin = create_array_binary(FetchTopicList),
  #          CommonPartofRequest = common_part_of_request(ExKafka.Constants.RQ_FETCH, ?API_V_FETCH, CorrelationId, ClientId),
  #          FetchRequest = <<CommonPartofRequest/binary, ?REPLICA_ID:32/signed-integer, MaxWaitTime:32/integer, MinBytes:32/integer, FetchTopicBin/binary>>,
  #          FetchRequestSize = size(FetchRequest),
  #          <<FetchRequestSize:32/integer, FetchRequest/binary>>.
  #
  #
  #  ##%-------------------------------------------------------------------
  #  ##%             Fetch Response
  #  ##%-------------------------------------------------------------------
  #
  #  -spec fetch_response(Response::binary()) -> tuple().
  #
  #  fetch_response(<<NumberOfTopics:32/integer, Rest/binary >>) ->
  #            parse_fetch_topic_details(NumberOfTopics, Rest, []).
  #
  #
  #
  #
  #
  #
  #
  #
  #
  #
  #
  #  ##%-------------------------------------------------------------------
  #  ##%                         API/OFFSET FUNCTION
  #  ##%-------------------------------------------------------------------
  #
  #  ##  @doc The         OFFSET request
  #  ##           offset_request(0000000001, "ERLKAFKA", [
  #  ##                                                    {<<"testtopic1">>,
  #  ##                                                           [
  #  ##							      {0, 0, 20000},
  #  ##							      {1, 0, 20000}
  #  ##                                                           ]
  #  ##						      },
  #  ##                                                    {<<"testtopic2">>,
  #  ##                                                           {
  #  ##							      [0, 0, 20000],
  #  ##							      [1, 0, 20000]
  #  ##                                                           }
  #  ##						      }
  #  ##						     ]
  #  ##			     )
  #
  #
  #  ##
  #  ## -spec        offset_request(
  #  ##		       CorrelationId::integer(),
  #  ##                       ClientId::list(),
  #  ##                       Topics::list()) ->
  #  ##			      binary().
  #  ##
  #
  #
  #  offset_request ( CorrelationId,
  #                    ClientId,
  #                    Topics
  #                  ) ->
  #
  #      CommonPartOfRequest = common_part_of_request(ExKafka.Constants.RQ_OFFSET,
  #                                                   ?API_V_OFFSET,
  #                                                   CorrelationId,
  #                                                   ClientId),
  #
  #      VarOffsetRequest = var_part_of_offset_request(Topics),
  #
  #      OffsetRequest =
  #          <<CommonPartOfRequest/binary,
  #            ?REPLICA_ID:32/integer,
  #            VarOffsetRequest/binary>>,
  #
  #      OffsetRequestSize = size(OffsetRequest),
  #
  #      <<OffsetRequestSize:32/integer, OffsetRequest/binary>>.
  #
  #
  #  %-spec offset_response(
  #  %		       Data::binary()) ->
  #  %			      {TopicList}
  #
  #
  #  offset_response(Data) ->
  #      Topics = parse_topics_for_offset_request(Data),
  #      {Topics}.
  #
  #
  #
  #
  #
  #  ##%-------------------------------------------------------------------
  #  ##%                       SOCKET DATA PROCESSING FUNCTION
  #  ##%-------------------------------------------------------------------
  #
  #  ## parse_response_stream should return length consumed & a list of  correlationids and the specific response parsed
  #  ## WHERE POSSIBLE
  #  ##
  def parse_response_stream(data) do
    parse_response_stream(data, 0, [])
  end

  def parse_response_stream(<<>>, length_consumed, list_of_responses) do
    { length_consumed, list_of_responses }
  end
  
  def parse_response_stream(<<response_size :: size(32), corr_id :: size(32), rest :: binary>> = _data, length_consumed, list_of_responses) when :erlang.size(rest) >= response_size - 4 do
    #  %    io:format("Correlation Id ~w~n", [CorrId]),
    { request_type, response_fun } = get_response_type_n_fun_from_corrid(corr_id)
    response_size_without_correlation_id = response_size - 4

    <<response_data :: binary-size(response_size_without_correlation_id), rest_of_rest :: binary>> = rest
    response_fun_return = response_fun.(response_data)
    parse_response_stream(rest_of_rest, length_consumed + 4 + response_size, [ {corr_id, request_type, response_fun_return} |  list_of_responses ])
  end

  def parse_response_stream(_data, length_consumed, list_of_responses) do
    { length_consumed, list_of_responses }
  end

  def get_response_type_n_fun_from_corrid(corr_id) when ((corr_id >= startmd) and (corr_id <= endmd)) do
    {ExKafka.Constants.rq_metadata, &metadata_response/1}
  end

  #  def get_response_type_n_fun_from_corrid(corr_id) when  ((corr_id >= ExKafka.Constants.startpr) and (corr_id =< ex_kafka.Constants.endpr)) do
  #    {ExKafka.Constants.rq_produce, &produce_response/1}
  #  end
  #
  #  def get_response_type_n_fun_from_corrid(corr_id) when  ((corr_id >= ExKafka.Constants.startfe) and (corr_id =< ExKafka.Constants.endfe)) do
  #    {ExKafka.Constants.rq_fetch, &fetch_response/1}
  #  end
  #  
  #  def get_response_type_n_fun_from_corrid(corr_id) when  corr_id == 0 do
  #    {ExKafka.Constants.rq_offset, &offset_response/1}
  #  end

  #
  #
  #
  #
  #
  #
  #  ##%-------------------------------------------------------------------
  #  ##%                         INTERNAL  FUNCTIONS
  #  ##%-------------------------------------------------------------------
  #
  def common_part_of_request(request_type, api_version, correlation_id, client_id) do
    client_id_size = :erlang.size(client_id)
    <<request_type :: size(16), api_version :: size(16), correlation_id :: size(32),
    client_id_size :: size(16), client_id :: binary>>
  end
  #
  #
  #
  #
  #
  #
  #
  #  ##%-------------------------------------------------------------------
  #  ##%                         INTERNAL METADATA FUNCTIONS
  #  ##%-------------------------------------------------------------------
  #

  def parse_brokers_info(<<broker_size::size(32), bin::binary>>) do
    parse_broker_info(broker_size, bin, [])
  end

  def parse_broker_info(0, bin, broker_list) do
    {broker_list, bin}
  end

  def parse_broker_info(broker_size, bin, broker_list) do
    {broker, remaining_bin} = parse_broker_info_details(bin)
    parse_broker_info((broker_size - 1), remaining_bin, [broker | broker_list])
  end

  def parse_broker_info_details(<<node_id::size(32), host_size::size(16), host::binary-size(host_size), port::size(32), bin::binary>>) do
    {%Broker{node_id: node_id, host: host, port: port}, bin}
  end

  def parse_topic_meta_data(<<topic_meta_data_size::size(32), bin::binary>>) do
    parse_topic_meta_data(topic_meta_data_size, bin, [])
  end

  def parse_topic_meta_data(0, bin, topic_meta_data_list) do
    {topic_meta_data_list, bin}
  end

  def  parse_topic_meta_data(topic_meta_data_size, bin, topic_meta_data_list) do
    {topic_meta_data, remainning_bin} = parse_topic_meta_data_details(bin)
    parse_topic_meta_data((topic_meta_data_size - 1), remainning_bin, [topic_meta_data | topic_meta_data_list])
  end

  def parse_topic_meta_data_details(<<topic_size::size(32), topic_name::binary-size(topic_size), bin::binary>>) do
    {partition_list, remainning_bin} = parse_partition_meta_data(bin)
    { %TopicMetaData{topic_name: topic_name, partition_meta_data_list: partition_list}, remainning_bin }
  end

  def parse_partition_meta_data(<<partition_size::size(32), bin::binary>>) do
    parse_partition_meta_data(partition_size, bin, [])
  end

  def parse_partition_meta_data(0, bin, partition_list) do
    { partition_list, bin }
  end

  def parse_partition_meta_data(partition_size, bin, partition_list) do
    { partition, remainning_bin } = parse_partition_meta_data_details(bin)
    parse_partition_meta_data((partition_size - 1), remainning_bin, [partition | partition_list])
  end

  def parse_partition_meta_data_details(<<partition_error_code::size(16), partition_id::size(32), leader_id::size(32), replica_size::size(32), bin::binary>>) do
    { replica_list, <<isr_size::size(32), isr_bin::binary>> } = retrieve_replica(replica_size, bin, [])
    { isr_list, remainning_bin } = retrieve_isr(isr_size, isr_bin, [])
    { %Partition{ partition_error_code: partition_error_code, partition_id: partition_id, leader_id: leader_id, replicas: replica_list, isr: isr_list}, remainning_bin }
  end

  def retrieve_replica(0, bin, replica_list) do
    { replica_list, bin }
  end

  def retrieve_replica(replica_size, bin, replica_list) do
    { replica, remainning_bin } = retrieve_replica(bin)
    retrieve_replica((replica_size - 1), remainning_bin, [replica | replica_list])
  end

  def retrieve_replica(<<replica::size(32), bin::binary>>) do
    {replica, bin}
  end

  def retrieve_isr(0, bin, isr_list) do
    {isr_list, bin}
  end

  def retrieve_isr(isr_size, bin, isr_list) do
    {isr, remainning_bin} = retrieve_isr_details(bin)
    retrieve_isr((isr_size - 1), remainning_bin, [isr | isr_list])
  end

  def retrieve_isr_details(<<isr::size(32), bin::binary>>) do
    { isr, bin }
  end

  #  ##%-------------------------------------------------------------------
  #  ##%                         INTERNAL PRODUCE REQUEST FUNCTIONS
  #  ##%-------------------------------------------------------------------
  #
  #
  #  var_part_of_produce_request (Topics) ->
  #
  #      TopicCount = length(Topics),
  #
  #      TopicRequests =
  #          lists:foldl(fun (X, A1) ->
  #                              {TopicName, PartitionInfo} = X,
  #
  #                              TopicNameSize = size(TopicName),
  #                              PartitionSize = length(PartitionInfo),
  #
  #                              VarP =
  #                                  lists:foldl(fun(Y, A2) ->
  #                                                      {PartitionId, ListMessages} = Y,
  #
  #                                                      MessageSet = create_message_set(ListMessages),
  #                                                      MessageSetSize = size(MessageSet),
  #
  #                                                      <<PartitionId:32/integer, MessageSetSize:32/integer, MessageSet/binary, A2/binary>>
  #                                              end,
  #                                              <<"">>,
  #                                              PartitionInfo),
  #                              <<TopicNameSize:16/integer, TopicName/binary, PartitionSize:32/integer, VarP/binary, A1/binary>>
  #                      end,
  #                      <<"">>,
  #                      Topics),
  #      <<TopicCount:32/integer, TopicRequests/binary>>.
  #
  #
  #
  #
  #
  #  parse_topics_for_produce_request (<<NumberOfTopics:32/integer, Bin/binary>>) ->
  #      parse_topics_for_produce_request(NumberOfTopics, Bin, []).
  #
  #  parse_topics_for_produce_request(0, _Bin, Topics) ->
  #      Topics;
  #  parse_topics_for_produce_request(RemainingCount, <<TopicLength:16/integer, TopicName:TopicLength/binary, PartitionLength:32/integer, RemainingBin/binary>>, Topics ) ->
  #      {ListPartitions, RestOfRemainingBin} = parse_partitions_for_produce_request (PartitionLength, RemainingBin),
  #      parse_topics_for_produce_request(RemainingCount-1, RestOfRemainingBin, [{TopicName, [ListPartitions]} | Topics]).
  #
  #  parse_partitions_for_produce_request(PartitionLength, RemainingBin) ->
  #      parse_partitions_for_produce_request(PartitionLength, RemainingBin, []).
  #
  #  parse_partitions_for_produce_request(0, Bin, ListPartitions) ->
  #      {ListPartitions, Bin};
  #
  #
  #  parse_partitions_for_produce_request(RemainingPartitions, <<PartitionId:32/integer, ErrorCode:16/integer, Offset:64/integer, RestOfBin/binary>>, ListPartitions) ->
  #      parse_partitions_for_produce_request(RemainingPartitions-1, RestOfBin, [{PartitionId, ErrorCode, Offset} | ListPartitions]).
  #
  #
  #
  #  create_message_set (ListMessages) ->
  #      RListMessages = lists:reverse(ListMessages),
  #      MessageSet =
  #          lists:foldl(fun (X, A) ->
  #                              Message=create_message(X),
  #  %			    io:format("~p~n", [Message]),
  #                              MessageSize = size(Message),
  #
  #                              <<?PRODUCE_OFFSET_MESSAGE:64/integer, MessageSize:32/integer,Message/binary, A/binary>>
  #                      end,
  #                      <<"">>,
  #                      RListMessages),
  #      <<MessageSet/binary>>.
  #
  #
  #  create_message({}) ->
  #      create_message({<<"">>, <<"">>});
  #
  #  create_message({Value}) ->
  #      create_message({<<"">>, Value});
  #
  #  create_message({Key, Value}) ->
  #      create_message({Key, Value, ?COMPRESSION_NONE, ?MAGIC_BYTE});
  #
  #  create_message({Key, Value, Compression}) ->
  #      create_message({Key, Value, Compression, ?MAGIC_BYTE});
  #
  #
  #
  #  create_message({Key, Value, Compression, Version}) ->
  #      KeyValueBin = create_key_value_bin(Key, Value),
  #      MessageBin = <<Version:8/integer, Compression:8/integer, KeyValueBin/binary>>,
  #      Crc32 = erlang:crc32(MessageBin),
  #      Message = <<Crc32:32/integer, MessageBin/binary>>,
  #
  #  %    io:format("~p ~p~n", [MessageBin, Crc32]),
  #  %    io:format("~p~n", [Message]),
  #      Message.
  #
  #
  #  create_key_value_bin(<<"">>, <<"">>) ->
  #          <<-1:32/big-signed-integer, -1:32/signed-integer>>;
  #
  #  create_key_value_bin(<<"">>, Value) ->
  #          ValueBin = Value,
  #          ValueSize = size(ValueBin),
  #          <<-1:32/big-signed-integer, ValueSize:32/big-signed-integer, ValueBin/binary>>;
  #
  #  create_key_value_bin(Key, Value) ->
  #          ValueBin = Value,
  #          ValueSize = size(ValueBin) ,
  #          KeyBin = Key,
  #          KeySize = size(KeyBin),
  #          <<KeySize:32/integer, KeyBin/binary, ValueSize:32/integer, ValueBin/binary>>.
  #
  #
  #
  #
  #
  #  ##%-------------------------------------------------------------------
  #  ##%                         INTERNAL OFFSET REQUEST FUNCTIONS
  #  ##%-------------------------------------------------------------------
  #
  #
  #  var_part_of_offset_request (Topics) ->
  #
  #      TopicCount = length(Topics),
  #
  #      TopicRequests =
  #          lists:foldl(fun (X, A1) ->
  #                              {TopicName, PartitionInfo} = X,
  #
  #
  #                              TopicNameSize = size(TopicName),
  #                              PartitionSize = length(PartitionInfo),
  #
  #                              VarP =
  #                                  lists:foldl(fun(Y, A2) ->
  #                                                      {PartitionId, Time, MaxNumberOfOffsets } =
  #                                                          var_part_of_partition_offset_request(Y),
  #                                                      <<PartitionId:32/integer, Time:64/integer, MaxNumberOfOffsets:32/integer, A2/binary>>
  #                                              end,
  #                                              <<"">>,
  #                                              PartitionInfo),
  #                              <<TopicNameSize:16/integer, TopicName/binary, PartitionSize:32/integer, VarP/binary, A1/binary>>
  #                      end,
  #                      <<"">>,
  #                      Topics),
  #      <<TopicCount:32/integer, TopicRequests/binary>>.
  #
  #
  #
  #  var_part_of_partition_offset_request ({PartitionId}) ->
  #      {PartitionId, ?LATEST_OFFSET, 1};
  #  var_part_of_partition_offset_request ({PartitionId, Time}) ->
  #      {PartitionId, Time, 1};
  #  var_part_of_partition_offset_request ({PartitionId, Time, MaxNumberOfOffsets}) ->
  #      {PartitionId, Time, MaxNumberOfOffsets }.
  #
  #
  #
  #
  #  parse_topics_for_offset_request (<<NumberOfTopics:32/integer, Bin/binary>>) ->
  #      parse_topics_for_offset_request(NumberOfTopics, Bin, []).
  #
  #  parse_topics_for_offset_request(0, _Bin, Topics) ->
  #      Topics;
  #
  #  parse_topics_for_offset_request(RemainingCount, <<TopicLength:16/integer, TopicName:TopicLength/binary, PartitionLength:32/integer, RemainingBin/binary>>, Topics ) ->
  #      {ListPartitions, RestOfRemainingBin} = parse_partitionoffsets_for_offset_request (PartitionLength, RemainingBin),
  #      parse_topics_for_offset_request(RemainingCount-1, RestOfRemainingBin, [{TopicName, ListPartitions} | Topics]).
  #
  #
  #  parse_partitionoffsets_for_offset_request(PartitionLength, RemainingBin) ->
  #      parse_partitionoffsets_for_offset_request(PartitionLength, RemainingBin, []).
  #
  #
  #  parse_partitionoffsets_for_offset_request(0, Bin, ListPartitions) ->
  #      {ListPartitions, Bin};
  #
  #
  #  parse_partitionoffsets_for_offset_request(RemainingPartitions,
  #                                     <<PartitionId:32/integer,
  #                                       ErrorCode:16/integer,
  #                                       OffsetsLength:32,
  #                                       RestOfBin/binary>>,
  #                                     ListPartitions) ->
  #      {ListOffsets, RestOfRemainingBin} = parse_offsets_for_offset_requests(OffsetsLength, RestOfBin, []),
  #      parse_partitionoffsets_for_offset_request(RemainingPartitions-1, RestOfRemainingBin, [{PartitionId, ErrorCode, ListOffsets} | ListPartitions]).
  #
  #  parse_offsets_for_offset_requests(0, RestOfBin, ListOffsets) ->
  #      {ListOffsets, RestOfBin};
  #  parse_offsets_for_offset_requests(RemainingOffsets, <<Offset:64/integer, RestOfBin/binary>>, ListOffsets) ->
  #      parse_offsets_for_offset_requests(RemainingOffsets -1, RestOfBin, [Offset | ListOffsets]).
  #
  #  ## Parsed payload response
  #
  #  -spec parse_fetch_topic_details(NumberOfTopics::integer(), TopicBin::binary(), Data::list()) -> tuple.
  #
  #  parse_fetch_topic_details(0, _ , Data) ->
  #          Data;
  #  parse_fetch_topic_details(NumberOfTopics, TopicBin, Data) ->
  #          ## Parse Topic Details
  #      <<TopicNameSize:16/integer, TopicName:TopicNameSize/binary, NumberOfPartitions:32/integer, PartitionBin/binary>> = TopicBin,
  #          {RemainningBin, PartitionList} = parse_fetch_partition_details(NumberOfPartitions, PartitionBin, []),
  #          parse_fetch_topic_details((NumberOfTopics - 1), RemainningBin, [{TopicName, PartitionList} | Data ]).
  #
  #  -spec parse_fetch_partition_details(NumberOfPartitions::integer(), PartitionBin::binary(), Data::list()) -> tuple.
  #
  #  parse_fetch_partition_details(0, PartitionBin, Data) ->
  #          {PartitionBin, Data};
  #  parse_fetch_partition_details(NumberOfPartitions, PartitionBin, Data) ->
  #          <<Partition:32/integer, ErrorCode:16/integer, HighwaterMarkOffset:64/integer, MessageSetSize:32/integer, Bin/binary>> = PartitionBin,
  #          <<MessageBin:MessageSetSize/binary, RemainningBin/binary>> = Bin,
  #      _ = {HighwaterMarkOffset, ErrorCode},
  #      MessageList = parse_fetch_message_details(MessageBin, []),
  #      parse_fetch_partition_details((NumberOfPartitions- 1), RemainningBin, [{Partition, MessageList} | Data]).
  #
  #  -spec parse_fetch_message_details(MessageBin::binary(), Data::list()) -> list.
  #
  #  parse_fetch_message_details(<<>>, Data) -> Data;
  #  parse_fetch_message_details(MessageBin, Data) ->
  #          <<OffSet:64/integer, MessageSize:32/integer,CRC32:32/integer, MagicByte:8/integer, Attributes:8/integer, KeySize:32/signed-integer, KeyBin/binary>> = MessageBin,
  #          _ = {MessageSize, CRC32, MagicByte, Attributes},
  #          {Key, ValueBin} = parse_bytes(KeySize, KeyBin),
  #          <<ValueSize:32/signed-integer, Values/binary>> = ValueBin,
  #          {Value, RemainningBin} = parse_bytes(ValueSize, Values),
  #          parse_fetch_message_details(RemainningBin, [{OffSet, Key, Value} | Data]).
  #
  #  parse_bytes(-1, Bin) ->
  #          {<<>>, Bin};
  #  parse_bytes(BytesSize, Bin) ->
  #          <<Bytes:BytesSize/binary, RemainningBin/binary>> = Bin,
  #          {Bytes, RemainningBin}.
  #
  #  ## @doc Create binary value as per kafka protocol from string.
  #  -spec create_string_binary(StringValue::list()) -> binary.
  #  create_string_binary([]) ->
  #          <<-1:16/signed-integer>>;
  #  create_string_binary(StringValue) ->
  #          StringBin = list_to_binary(StringValue),
  #          StringSize = size(StringBin),
  #          <<StringSize:16/integer, StringBin/binary>>.
  #
  #  ## @doc Create binary value as per kafka protocol from list.
  #  -spec create_array_binary(ListValues::list()) -> binary.
  #  create_array_binary([]) ->
  #          <<-1:32/signed-integer>>;
  #  create_array_binary(ListValues) ->
  #          NumberOfValues = length(ListValues),
  #          ValueBin = list_to_binary(ListValues),
  #          <<NumberOfValues:32/integer, ValueBin/binary>>.
end
