from cowboycushion.multiprocessing_limiter import RedisMultiprocessingLimiter


def gather_data(args, DataImpl):
    data_client = RedisMultiprocessingLimiter(
        DataImpl(),
        args.batch_poll_timeout,
        args.num_calls_per_batch,
        args.seconds_per_batch,
        args.redis_host,
        args.redis_port,
        args.redis_db,
        args.pool_size
    )
    jobs = [
        # Hacky but wait until we actually care about looking at months
        data_client.get_eps(args.ticker, 5 if int(args.time_length) < 5 else 10),
        data_client.get_prices(args.ticker, args.time_length)
    ]
    data_client.close()
    data_client.join()
    dfs = [job.get() for job in jobs]
    # just keep it like this until we need otherwise
    merged = merge_data_frames(*dfs)


def merge_unequal_data_frames(dfa, dfb):
    """
    Given the assumptions of our system, that all dates will revolve around price data,
    we want to take unequal data frames like eps and overlay them on the price data
    frame.
    """
    dfa_idx = dfa.index.values
    dfb_idx = dfb.index.values
    if len(dfa) > len(dfb):
        pass
    else:
        pass
    # For the moment string comparisons will do just fine
    import pdb; pdb.set_trace()


def merge_data_frames(dfa, dfb):
    if len(dfa) == len(dfb) and (dfa.index.values == dfb.index.values).all():
        return dfa.combine_first(dfb)
    elif len(dfa) != len(dfb):
        return merge_unequal_data_frames(dfa, dfb)
    else:
        import pdb; pdb.set_trace()
        # Is a corner case
