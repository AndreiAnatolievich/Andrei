# PANDAS
df_pd = pd.read_csv("data.csv")


# POLARS
df_pl = pl.read_csv("data.csv")

# PANDAS
df_pd_italy = pd.read_csv("data.csv")
df_pd_italy["date"] = pd.to_datetime(df_pd_italy["date"])
df_pd_italy = df_pd_italy[df_pd_italy["location"] == "Italy"]
df_pd_italy = df_pd_italy[["date", "sales"]]


# POLARS
df_pl_italy = (pl.read_csv("data.csv")
      .with_columns(pl.col("date").str.to_date("%Y-%m-%d %H:%M:%S"))
      .filter(pl.col("location") == "Italy")
      .select(pl.col(["date", "sales"])))


# PANDAS
(df_pd.groupby([df_pd["date"].dt.year, 
    df_pd["location"]]).agg(total_sales=("sales", "sum"),
    avg_sales=("sales", "mean")).reset_index().sort_values(by=["date", "total_sales"], ascending=[True, False]))

# POLARS
(df_pl.group_by([pl.col("date").dt.year(), 
    pl.col("location")]).agg(pl.sum("sales").alias("total_sales"),
    pl.mean("sales").alias("avg_sales")).sort(by=["date", "total_sales"], descending=[False, True]))

# PANDAS
df_pd = pd.read_csv("data.csv")
df_pd["date"] = pd.to_datetime(df_pd["date"])

df_pd_res = (df_pd.groupby([df_pd["date"].dt.year, 
    df_pd["location"]]).agg(total_sales=("sales", "sum"),
    avg_sales=("sales", "mean")).reset_index()
        .sort_values(by=["date", "total_sales"], ascending=[True, False])
)


# POLARS
df_pl_res = (
    pl.read_csv("data.csv")
        .with_columns(pl.col("date").str.to_date("%Y-%m-%d %H:%M:%S"))
        .group_by([pl.col("date").dt.year(), pl.col("location")])
        .agg(
            pl.sum("sales").alias("total_sales"),
            pl.mean("sales").alias("avg_sales")
        )
        .sort(by=["date", "total_sales"], descending=[False, True])
)


# POLARS WITH LAZY EVALUATION
df_pl_lazy_query = (
    pl.scan_csv("data.csv")
        .with_columns(pl.col("date").str.to_date("%Y-%m-%d %H:%M:%S"))
        .group_by([pl.col("date").dt.year(), pl.col("location")])
        .agg(
            pl.sum("sales").alias("total_sales"),
            pl.mean("sales").alias("avg_sales")
        )
        .sort(by=["date", "total_sales"], descending=[False, True])
)

df_pl_lazy_res = df_pl_lazy_query.collect()
