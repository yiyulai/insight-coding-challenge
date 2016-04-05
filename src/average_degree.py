#!/usr/bin/env python3

import sys
import json
import time
import datetime
from itertools import combinations

graph_dict = dict()
graph_first_time = 1
curr_sliding_bucket = []
latest_timestamp = 0.0
ts_threshold = 60

def check_sliding_win(tstamp):
    global curr_sliding_bucket
    tmp_sliding_bucket = curr_sliding_bucket[:]
    removed_idx_list = []
    for idx, s in enumerate(curr_sliding_bucket):
        s_timestamp = s[0]
        # evict tweets fall outside time window
        if s_timestamp <= (tstamp - ts_threshold):
            removed_idx_list.append(idx)
    for r in sorted(removed_idx_list, reverse=True):
        del tmp_sliding_bucket[r]
    updated_sliding_bucket = tmp_sliding_bucket[:]
    return (updated_sliding_bucket, removed_idx_list)

def check_affected_tags(updated_tags, removed_tags):
    affected_tags = []
    for r in removed_tags:
        com_list = list(combinations(r[1], 2))
        for c in com_list:
            c_flag = 0
            for u in updated_tags:
                if set(c).issubset(set(u[1])):
                    c_flag = 1
                    break
            # the hashtag pair does not exist in other valid tweets and need to be removed for graph
            if c_flag == 0:
                affected_tags.append(c)
    return affected_tags

def update_graph(ulist, alist, ilist):
    global graph_dict, graph_first_time
    # when it is the first time to generate hashtag graph
    if graph_first_time == 1:
        graph_first_time = 0
        for idx, u in enumerate(ulist):
            for t_idx, tag in enumerate(u[1]):
                curr_neighbor_list = u[1][:]
                del curr_neighbor_list[t_idx]
                if len(curr_neighbor_list) > 0:
                    try:
                        # update the neighbors for current hashtag
                        graph_dict[tag] = list(set(curr_neighbor_list) | set(graph_dict[tag]))
                    except KeyError:
                        graph_dict[tag] = curr_neighbor_list
    else:
        # first update -- insert new tags
        for idx, i in enumerate(ilist):
            curr_neighbor_list = ilist[:]
            del curr_neighbor_list[idx]
            if len(curr_neighbor_list) > 0:
                try:
                    # update the neighbors for current hashtag
                    graph_dict[i] = list(set(curr_neighbor_list) | set(graph_dict[i]))
                except KeyError:
                    graph_dict[i] = curr_neighbor_list
        # second update -- remove tags
        for a in alist:
            try:
                graph_dict[a[0]] = [x for x in graph_dict[a[0]] if x != a[1]]
                if len(graph_dict[a[0]]) == 0:
                    graph_dict.pop(a[0], None)
            except KeyError:
                pass
            try:
                graph_dict[a[1]] = [x for x in graph_dict[a[1]] if x != a[0]]
                if len(graph_dict[a[1]]) == 0:
                    graph_dict.pop(a[1], None)
            except KeyError:
                pass

def print_graph():
    print("\nCurrent Graph:")
    for key in graph_dict.keys():
        print("key= {0}, neighbors= {1}".format(key, graph_dict[key]))

def get_truncated_float(f_result, digits):
    f_tmp = '%.12f' % f_result
    x, y, z = f_tmp.partition('.')
    return '.'.join([x, (z+'0'*digits)[:digits]])

def update_average_degree():
    global graph_dict
    total_num_nodes = len(graph_dict.keys())
    total_degrees = 0
    for key in graph_dict.keys():
        total_degrees += len(graph_dict[key])
    try:
        return get_truncated_float(total_degrees/total_num_nodes, 2)
    except ZeroDivisionError:
        return '0.00'

def get_twitter_data(ifile, writer):
    global curr_sliding_bucket, latest_timestamp
    with open(ifile) as twitter_file:
        for idx, line in enumerate(twitter_file):
            try:
                tdata = []
                twitter_data = json.loads(line)
                time_str = twitter_data['created_at']
                # strptime %z is supported in Python 3.2+
                curr_timestamp = time.mktime(datetime.datetime.strptime(time_str, '%a %b %d %H:%M:%S %z %Y').timetuple())
                tdata.append(curr_timestamp)
                i = 0
                tag_list = []
                while True:
                    try:
                        tag_list.append(twitter_data['entities']['hashtags'][i]['text'])
                        i += 1
                    except IndexError:
                        # update latest timestamp of tweet
                        if curr_timestamp <= (latest_timestamp - ts_threshold):
                            # ignore the tweet which falls outside the window
                            pass
                        else:
                            if curr_timestamp > latest_timestamp:
                                latest_timestamp = curr_timestamp
                            # remove duplicates in tag list
                            tag_list = list(set(tag_list))
                            tdata.append(tag_list)
                            # add the tweet to current bucket
                            curr_sliding_bucket.append(tdata)
                            # check the time sliding window
                            (updated_sliding_bucket, removed_idx_list) = check_sliding_win(latest_timestamp)
                            # retrieve list of removed tags
                            removed_tag_list = [curr_sliding_bucket[r] for r in removed_idx_list]
                            # check affected tags
                            affected_tag_list = check_affected_tags(updated_sliding_bucket, removed_tag_list)
                            # update the graph
                            update_graph(updated_sliding_bucket, affected_tag_list, tag_list)
                        # print the current graph
                        #print_graph()
                        # update the average degree
                        updated_result = update_average_degree()
                        # write the updated average degree to output file
                        writer.write(updated_result+"\n")
                        break
            except (ValueError, KeyError):
                continue

if __name__ == '__main__':
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    writer = open(output_file, 'w')
    get_twitter_data(input_file, writer)
    writer.close()
    print("\n")
