import os
from datetime import datetime
from sqlalchemy import (
    create_engine, Column, Integer, Text, Float, Boolean,
    DateTime, UniqueConstraint, text
)
from sqlalchemy.orm import DeclarativeBase, Session

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "sqlite:///onpe.db"
)
# Render provides postgres:// but SQLAlchemy needs postgresql://
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
    pool_pre_ping=True,
)


class Base(DeclarativeBase):
    pass


class Mesa(Base):
    __tablename__ = "mesas"
    id            = Column(Integer, primary_key=True)
    codigo_mesa   = Column(Text, nullable=False, unique=True)
    nombre_local  = Column(Text)
    centro_poblado= Column(Text)
    id_ubigeo     = Column(Integer)
    total_electores      = Column(Integer)
    total_votos_emitidos = Column(Integer)
    total_votos_validos  = Column(Integer)
    pct_participacion    = Column(Float)
    estado_acta   = Column(Text)
    codigo_estado = Column(Text)
    scraped_at    = Column(DateTime, default=datetime.utcnow)


class Resultado(Base):
    __tablename__ = "resultados"
    __table_args__ = (
        UniqueConstraint("codigo_mesa", "id_eleccion", "codigo_partido"),
    )
    id             = Column(Integer, primary_key=True)
    codigo_mesa    = Column(Text, nullable=False)
    id_eleccion    = Column(Integer, nullable=False)
    nombre_eleccion= Column(Text)
    codigo_partido = Column(Text)
    nombre_partido = Column(Text)
    votos          = Column(Integer, default=0)
    pct_validos    = Column(Float, default=0)
    pct_emitidos   = Column(Float, default=0)
    es_partido     = Column(Boolean, default=True)


class ScraperState(Base):
    __tablename__ = "scraper_state"
    id             = Column(Integer, primary_key=True, default=1)
    status         = Column(Text, default="idle")
    current_code   = Column(Integer, default=0)
    range_start    = Column(Integer, default=1)
    range_end      = Column(Integer, default=89999)
    total_scanned  = Column(Integer, default=0)
    total_found    = Column(Integer, default=0)
    started_at     = Column(DateTime)
    updated_at     = Column(DateTime)


ELECTION_NAMES = {
    10: "Presidencial",
    12: "Parlamento Andino",
    13: "Senadores DEU",
    14: "Senadores DEM",
    15: "Senadores 33",
    20: "Diputados",
}


def init_db():
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        if not s.get(ScraperState, 1):
            s.add(ScraperState(id=1))
            s.commit()


def get_session():
    return Session(engine)


# ── Statistics queries ──────────────────────────────────────────────────────

def stats_overview():
    with get_session() as s:
        total_mesas   = s.execute(text("SELECT COUNT(*) FROM mesas")).scalar()
        total_electores = s.execute(text("SELECT COALESCE(SUM(total_electores),0) FROM mesas")).scalar()
        total_emitidos  = s.execute(text("SELECT COALESCE(SUM(total_votos_emitidos),0) FROM mesas")).scalar()
        total_validos   = s.execute(text("SELECT COALESCE(SUM(total_votos_validos),0) FROM mesas")).scalar()
        avg_part = s.execute(text("SELECT COALESCE(AVG(pct_participacion),0) FROM mesas")).scalar()
        contabilizadas = s.execute(text("SELECT COUNT(*) FROM mesas WHERE codigo_estado='C'")).scalar()
        pendientes     = s.execute(text("SELECT COUNT(*) FROM mesas WHERE codigo_estado!='C' OR codigo_estado IS NULL")).scalar()
        state = s.get(ScraperState, 1)
        return {
            "total_mesas": total_mesas,
            "total_electores": int(total_electores or 0),
            "total_emitidos": int(total_emitidos or 0),
            "total_validos": int(total_validos or 0),
            "avg_participacion": round(float(avg_part or 0), 2),
            "contabilizadas": contabilizadas,
            "pendientes": pendientes,
            "scraper": {
                "status": state.status if state else "idle",
                "current_code": state.current_code if state else 0,
                "range_end": state.range_end if state else 89999,
                "total_scanned": state.total_scanned if state else 0,
                "total_found": state.total_found if state else 0,
            }
        }


def stats_parties(id_eleccion=10, limit=30):
    with get_session() as s:
        rows = s.execute(text("""
            SELECT nombre_partido, SUM(votos) as total, AVG(pct_validos) as pct
            FROM resultados
            WHERE id_eleccion=:e AND es_partido=1
            GROUP BY nombre_partido
            ORDER BY total DESC
            LIMIT :l
        """), {"e": id_eleccion, "l": limit}).fetchall()
        return [{"partido": r[0], "votos": int(r[1] or 0), "pct": round(float(r[2] or 0), 2)} for r in rows]


def stats_participation_buckets():
    with get_session() as s:
        rows = s.execute(text("""
            SELECT
              CASE
                WHEN pct_participacion < 50 THEN '0-50%'
                WHEN pct_participacion < 60 THEN '50-60%'
                WHEN pct_participacion < 70 THEN '60-70%'
                WHEN pct_participacion < 80 THEN '70-80%'
                WHEN pct_participacion < 90 THEN '80-90%'
                ELSE '90-100%'
              END as bucket,
              COUNT(*) as cnt
            FROM mesas
            GROUP BY bucket
            ORDER BY bucket
        """)).fetchall()
        return [{"bucket": r[0], "count": int(r[1])} for r in rows]


def stats_acta_status():
    with get_session() as s:
        rows = s.execute(text("""
            SELECT COALESCE(codigo_estado,'?') as estado, COUNT(*) as cnt
            FROM mesas GROUP BY estado ORDER BY cnt DESC
        """)).fetchall()
        return [{"estado": r[0], "count": int(r[1])} for r in rows]


def stats_elecciones():
    with get_session() as s:
        rows = s.execute(text("""
            SELECT id_eleccion, nombre_eleccion, SUM(votos) as total
            FROM resultados WHERE es_partido=1
            GROUP BY id_eleccion, nombre_eleccion ORDER BY id_eleccion
        """)).fetchall()
        return [{"id": r[0], "nombre": r[1], "total_votos": int(r[2] or 0)} for r in rows]
