"""
pipeline/logger.py
Logging estruturado usando structlog.
Todos os módulos do pipeline importam daqui.
"""

import logging
import sys
import structlog


def setup_logging(log_level: str = "INFO") -> None:
    """Configura o structlog com saída JSON estruturada."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )

    # Bibliotecas de terceiros são muito verbosas em INFO — sobem só warnings.
    for noisy in ("pika", "sqlalchemy.engine"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level.upper(), logging.INFO)
        ),
        context_class=dict,
        # PrintLoggerFactory escreve JSON direto no stdout (sem stdlib).
        # Por isso o nome do logger é vinculado em get_logger(), e NÃO via
        # o processador stdlib add_logger_name (que exige um logger com .name).
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    """Retorna um logger nomeado e configurado (campo `logger` no JSON)."""
    setup_logging()
    return structlog.get_logger().bind(logger=name)