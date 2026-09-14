const dbName = process.env.MONGO_INITDB_DATABASE || "recipe_ai";
const appUser = process.env.MONGO_APP_USER;
const appPassword = process.env.MONGO_APP_PASSWORD;

if (!appUser || !appPassword) {
  throw new Error("MONGO_APP_USER and MONGO_APP_PASSWORD are required");
}

const appDb = db.getSiblingDB(dbName);
appDb.createUser({
  user: appUser,
  pwd: appPassword,
  roles: [{ role: "readWrite", db: dbName }]
});
