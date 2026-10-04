export const config = {
  databaseUrl:
    process.env.DATABASE_URL ?? 'postgres://pgqueue:pgqueue@localhost:5432/pgqueue',
  port: Number(process.env.PORT ?? 3000),
  dbPoolMax: Number(process.env.DB_POOL_MAX ?? 10),
};
