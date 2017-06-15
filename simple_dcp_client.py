#!/usr/bin/env python

import argparse
import pprint
import time

from lib.dcp_bin_client import DcpClient, Operation
from lib.mc_bin_client import MemcachedClient as McdClient
from constants import *

def add_stream(args):
    print 'Creating DCP Client Object to', args.host + ":" + str(args.port)
    dcp_client = DcpClient(args.host, args.port, timeout=9999999)

    print 'Performing SASL Auth to Bucket'
    response = dcp_client.sasl_auth_plain(args.bucket, args.password)
    
    print 'Sending open connection (consumer)...',
    response = dcp_client.open_producer("haiko1980",xattr=True)
    assert response['status'] == SUCCESS
    print "success"

    print 'Enabling DCP noop...',
    dcp_client.general_control('enable_noop', 'true')
    dcp_client.general_control('set_noop_interval', '2')
    print "success"
    
    print 'Sending add stream request...',
    vb = 321
    end_seq_no = 0xffffffffffffffff
    op = dcp_client.stream_req(vb, 0, 0, end_seq_no, 0, 0)
    print "success"

    op2 = dcp_client.stream_req(vb + 1, 0, 0, end_seq_no, 0, 0)

    noop_count = 0
    while True:
        if op.has_response():
            response = op.next_response()
            print 'Got response: ', hex(response['opcode'])
            if response['opcode'] == CMD_STREAM_REQ:
                assert response['status'] == SUCCESS
            elif response['opcode'] == CMD_MUTATION:
                vb = response['vbucket']
                key = response['key']
                seqno =  response['value']
                xattrs = response['xattrs']
                print 'VB: %d got key %s with seqno %s' % (vb, key, xattrs)
            elif response['opcode'] == CMD_NOOP:
                dcp_client.ack_noop(response['opaque'])
                noop_count += 1
                if noop_count == 3:
                    print 'Closing DCP stream'
                    print dcp_client.close_stream(vb)
                    print dcp_client.close_stream(vb+1)

        if op2.has_response():
            response = op2.next_response()
            print 'Got response: ', hex(response['opcode'])
            if response['opcode'] == CMD_STREAM_REQ:
                assert response['status'] == SUCCESS
            elif response['opcode'] == CMD_MUTATION:
                vb = response['vbucket']
                key = response['key']
                seqno =  response['value']
                xattrs = response['xattrs']
                print 'VB: %d got key %s with seqno %s' % (vb, key, xattrs)
            elif response['opcode'] == CMD_NOOP:
                dcp_client.ack_noop(response['opaque'])
                noop_count += 1
                if noop_count == 3:
                    print 'Closing DCP stream'
                    print dcp_client.close_stream(vb)
                    print dcp_client.close_stream(vb+1)

        else:
            print 'No response'

    dcp_client.shutdown()
    mcd_client.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simple DCP Client')
    parser.add_argument('--host', default='localhost')
    parser.add_argument('--port', default=11210, type=int)
    parser.add_argument('--bucket', default='default')
    parser.add_argument('--label', default='simple_dcp_client')
    parser.add_argument('--user', default='default')
    parser.add_argument('--password', default='')
    args = parser.parse_args()

    add_stream(args)
