from alcmy_models import Base, Fact, TradeDim, PkgDim
from alcmy_conn import engine


print("Creating Tables >>>> ")
Base.metadata.create_all(bind=engine)
