from cowboycushion.multiprocessing_limiter import RedisMultiprocessingLimiter
from numpy import append, array
from pandas import DataFrame


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
    return merge_data_frames(*dfs)


def merge_unequal_data_frames(dfa, dfb):
    """
    Given the assumptions of our system, that all dates will revolve around price data,
    we want to take unequal data frames like eps and overlay them on the price data
    frame.

    We do not have the prettiest algorithm for doing this but it works
    """
    def find_closest_idx(greater_idx, lesser_idx, idx):
        val = greater_idx[idx]
        for i, lesser_val in enumerate(lesser_idx):
            if val == lesser_val:
                return i
            elif i > 0 and lesser_idx[i - 1] < val and lesser_val > val:
                return i - 1
            elif i == len(lesser_idx) - 1:
                return i
        else:
            import pdb; pdb.set_trace()
            # Corner case unexpected

    if len(dfa) > len(dfb):
        greater = dfa
        lesser = dfb
    else:
        greater = dfb
        lesser = dfa

    mapped_array = array([])
    for i, k in enumerate(greater.values):
        closest_idx = find_closest_idx(greater.index.values, lesser.index.values, i)
        mapped_array = append(mapped_array, lesser.values[closest_idx])

    return greater.combine_first(
        DataFrame(mapped_array, index=greater.index.values, columns=[lesser.columns[0]])
    )


def merge_data_frames(dfa, dfb):
    if len(dfa) == len(dfb) and (dfa.index.values == dfb.index.values).all():
        return dfa.combine_first(dfb)
    elif len(dfa) != len(dfb):
        return merge_unequal_data_frames(dfa, dfb)
    else:
        import pdb; pdb.set_trace()
        # Is a corner case
