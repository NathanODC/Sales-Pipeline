from __future__ import annotations

from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import ForeignKey, Text
from typing import List


class Base(DeclarativeBase):
    pass


class Fact(Base):

    __tablename__ = "fact_sales"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    date: Mapped[str] = mapped_column(nullable=False)
    ce_brand_flvr: Mapped[str] = mapped_column(nullable=False)
    brand_nm: Mapped[str] = mapped_column(nullable=False)
    btlr_org_lvl_c_desc: Mapped[str] = mapped_column(nullable=False)
    chnl_group: Mapped[str] = mapped_column(nullable=False)
    trade_id: Mapped[int] = mapped_column(ForeignKey("trade_chnl_dim.id"))
    trade_dim: Mapped["TradeDim"] = relationship(back_populates="fact_rel")
    pkg_id: Mapped[int] = mapped_column(ForeignKey("pkg_dim.id"))
    pkg_dim: Mapped["PkgDim"] = relationship(back_populates="fact_rel")
    tsr_pckg_num: Mapped[str] = mapped_column(nullable=False)
    volume: Mapped[float] = mapped_column(nullable=False)
    year: Mapped[int] = mapped_column(nullable=False)
    month: Mapped[int] = mapped_column(nullable=False)
    period: Mapped[int] = mapped_column(nullable=False)


class TradeDim(Base):

    __tablename__ = "trade_chnl_dim"

    id: Mapped[int] = mapped_column(primary_key=True)
    trade_chnl_desc: Mapped[str] = mapped_column(nullable=False)
    trade_group_desc: Mapped[str] = mapped_column(nullable=False)
    trade_type_desc: Mapped[str] = mapped_column(nullable=False)
    fact_rel: Mapped["Fact"] = relationship(back_populates="trade_dim")


class PkgDim(Base):

    __tablename__ = "pkg_dim"

    id: Mapped[int] = mapped_column(primary_key=True)
    pkg_cat: Mapped[str] = mapped_column(nullable=False)
    pkg_cat_desc: Mapped[str] = mapped_column(nullable=False)
    fact_rel: Mapped["Fact"] = relationship(back_populates="pkg_dim")
