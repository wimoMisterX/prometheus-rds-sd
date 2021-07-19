import argparse
import boto3
import flask
import logging

from cachetools import cached, TTLCache
from gevent.pywsgi import WSGIServer

logger = logging.getLogger(__name__)


@cached(cache=TTLCache(maxsize=1024, ttl=600))
def role_arn_to_rds_client(role_arn):
    client = boto3.client('sts')
    response = client.assume_role(
        RoleArn=role_arn,
        RoleSessionName='PrometheusRdsServiceDisocvery',
    )
    return boto3.Session(
        aws_access_key_id=response['Credentials']['AccessKeyId'],
        aws_secret_access_key=response['Credentials']['SecretAccessKey'],
        aws_session_token=response['Credentials']['SessionToken']
    ).client("rds")


def run_http_server(port):
    app = flask.Flask(__name__)

    default_rds_client = boto3.client("rds")

    @app.route("/", methods=["GET"])
    def discover():
        role_arn = flask.request.args.get("role_arn")
        rds_client = role_arn_to_rds_client(role_arn) if role_arn else default_rds_client

        db_clusters = [
            {
                "targets": [f"{db_cluster['Endpoint']}:{db_cluster['Port']}",],
                "labels": {
                    "__meta_rds_instance_id": db_cluster["DBClusterIdentifier"],
                    "__meta_rds_engine": db_cluster["Engine"],
                    "__meta_rds_engine_version": db_cluster["EngineVersion"],
                    **{
                        f"__meta_rds_tag_{tag['Key']}": tag["Value"]
                        for tag in db_cluster["TagList"]
                    },
                },
            }
            for db_cluster in rds_client.describe_db_clusters()["DBClusters"]
        ]

        db_instances = [
            {
                "targets": [f"{db_instance['Endpoint']['Address']}:{db_instance['Endpoint']['Port']}",],
                "labels": {
                    "__meta_rds_instance_id": db_instance["DBInstanceIdentifier"],
                    "__meta_rds_engine": db_instance["Engine"],
                    "__meta_rds_engine_version": db_instance["EngineVersion"],
                    **{
                        f"__meta_rds_tag_{tag['Key']}": tag["Value"]
                        for tag in db_instance["TagList"]
                    },
                },
            }
            for db_instance in rds_client.describe_db_instances()["DBInstances"]
            if not db_instance.get("DBClusterIdentifier")
        ]

        return flask.jsonify(db_clusters + db_instances)

    http_server = WSGIServer(("", port), app, log=logger)
    http_server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(
        handlers=[logging.StreamHandler()],
        level=logging.INFO,
        format="%(asctime)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--port",
        type=int,
        help="Port to run server",
        default=6748,
    )
    opts = arg_parser.parse_args()

    run_http_server(opts.port)
