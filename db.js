const fs = require("fs");
const sqlite3 = require("sqlite3").verbose();
const filepath = "./order.db";

function connectToDatabase() {
  if (fs.existsSync(filepath)) {
    return new sqlite3.Database(filepath);
  } else {
    const db = new sqlite3.Database(filepath, (error) => {
      if (error) {
        return console.error(error.message);
      }
      createTable(db);
      console.log("Connected to the database successfully");
    });
    return db;
  }
}

function createTable(db) {
  db.exec(`
  CREATE TABLE orders
  (
    ORDER_ID       VARCHAR(100),
    WHERE_FROM     VARCHAR(100),
    PRODUCT_NAME   VARCHAR(100),
    COUNT          INT,
    BRAND_NAME     VARCHAR(100)
  )
`);
}

module.exports = connectToDatabase();