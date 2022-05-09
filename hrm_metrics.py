"""Prometheus metrics collection for the Huygens Remote Manager (HRM)."""

import signal
import sys
import time

from loguru import logger as log

from prometheus_client import Gauge, CollectorRegistry, start_http_server

from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker, scoped_session

from hrm_config import SERVER, USER, PASS, DBNAME


SA_ECHO = False
METRICS_PORT = 9101
UPDATE_FREQUENCY = 15
REGISTRY = CollectorRegistry()


class HrmDbConnection:
    def __init__(self):

        self.engine = create_engine(
            f"mysql+pymysql://{USER}:{PASS}@{SERVER}/{DBNAME}", echo=SA_ECHO
        )
        self.metadata = MetaData(self.engine)
        self.engine.connect()

        session_obj = sessionmaker(bind=self.engine)
        self.session = scoped_session(session_obj)
        self.query = self.session.query  # pylint: disable-msg=no-member

        self.base = automap_base()
        self.base.prepare(self.engine, reflect=True)

        # database tables we want to query later on:
        self.t_username = self.base.classes.username
        self.t_job_queue = self.base.classes.job_queue
        self.t_statistics = self.base.classes.statistics

        self.q_users = self.query(self.t_username)
        self.q_stats = self.query(self.t_statistics)
        self.q_queue = self.query(self.t_job_queue)

        log.success(f"Established connection to HRM db on {SERVER}.")

    def microscope_types(self):
        types = []
        for m_type in self.query(self.t_statistics.MicroscopeType).distinct():
            types.append(m_type[0])

        return types

    def input_formats(self):
        formats = []
        for in_format in self.query(self.t_statistics.ImageFileFormat).distinct():
            formats.append(in_format[0])
        return formats


class GaugeFromFunction(Gauge):
    def __init__(self, metric_function, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_function(metric_function)
        self.set(0)


def shutdown(signum, frame):
    log.debug(
        f"Shutdown requested with signal [{signum}] and frame ["
        f"{frame.f_code.co_filename}:{frame.f_code.co_name}:{frame.f_lineno}]."
    )
    print("Shutting down.")
    sys.exit(0)


def jobs_gauge(db_conn):
    _gauge = Gauge(
        name="huygens_rm_queued_jobs",
        documentation="The number of jobs in the HRM queue.",
        registry=REGISTRY,
        labelnames=["status"],
    )
    job_status_mapping = {
        "running": "started",
        "waiting": "queued",
        "broken": "broken",
        "kill": "kill",
    }
    _gauge.labels("total").set_function(db_conn.q_queue.count)
    for label_value, status in job_status_mapping.items():
        _gauge.labels(label_value).set_function(
            db_conn.q_queue.filter_by(status=status).count
        )
    return _gauge


def microscope_types_gauge(db_conn):
    _gauge = Gauge(
        name="huygens_rm_statistics",
        documentation="Statistics on all finished jobs of this HRM instance.",
        registry=REGISTRY,
        labelnames=["microscope_type"],
    )
    _gauge.labels("all").set_function(db_conn.q_stats.count)
    m_types = db_conn.microscope_types()
    for m_type in m_types:
        _gauge.labels(m_type).set_function(
            db_conn.q_stats.filter_by(MicroscopeType=m_type).count
        )
    return _gauge


def formats_gauge(db_conn):
    _gauge = Gauge(
        name="huygens_rm_decon_input_format",
        documentation="Statistics on the input file format of all finished jobs.",
        registry=REGISTRY,
        labelnames=["type"],
    )
    _gauge.labels("all").set_function(db_conn.q_stats.count)
    for in_format in db_conn.input_formats():
        _gauge.labels(in_format).set_function(
            db_conn.q_stats.filter_by(ImageFileFormat=in_format).count
        )
    return _gauge


def setup_metrics():
    db_conn = HrmDbConnection()

    metrics = []

    metrics.append(
        GaugeFromFunction(
            metric_function=db_conn.q_users.count,
            name="huygens_rm_users_total",
            documentation="The total number of user accounts",
            registry=REGISTRY,
        )
    )

    metrics.append(
        GaugeFromFunction(
            metric_function=db_conn.q_users.filter_by(status="a").count,
            name="huygens_rm_users_active",
            documentation="The number of user accounts with status 'active'",
            registry=REGISTRY,
        )
    )

    metrics.append(jobs_gauge(db_conn))
    metrics.append(microscope_types_gauge(db_conn))
    metrics.append(formats_gauge(db_conn))

    log.info(f"Microscope types in DB: {db_conn.microscope_types()}")
    log.info(f"Input file formats in DB: {db_conn.input_formats()}")

    return metrics


def run_metrics_collection():
    metrics = setup_metrics()

    collector_duration = Gauge(
        "huygens_rm_collector_duration_seconds",
        "Runtime of the metrics collector",
        registry=REGISTRY,
    )
    collector_duration.set(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    start_http_server(METRICS_PORT, registry=REGISTRY)
    log.success(f"Started HTTP server on port {METRICS_PORT}.")

    while True:
        time_start = time.perf_counter()
        for metric in metrics:
            try:
                metric.collect()
            except Exception as err:  # pylint: disable-msg=broad-except
                log.warning(f"Updating metric failed: {err}")

        time_delta = time.perf_counter() - time_start
        log.trace(f"{time_delta:.12f}")
        collector_duration.set(time_delta)
        time.sleep(UPDATE_FREQUENCY)


if __name__ == "__main__":
    run_metrics_collection()
