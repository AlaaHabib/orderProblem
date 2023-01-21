const fs = require("fs");
const { parse } = require("csv-parse");
const { stringify } = require("csv-stringify");
const db = require("./db");

fs.createReadStream("./order_log00.csv")
  .pipe(parse({ delimiter: ",", from_line: 2 }))
  .on("data", function (row) {
    db.serialize(function () {
      db.run(
        `INSERT INTO orders VALUES (?, ?, ? , ?, ?)`,
        [row[0], row[1], row[2], row[3], row[4]],
        function (error) {
          if (error) {
            return console.log(error.message);
          }
        }
      );
    });
  })
  .on("end", function order0() {
    const filename = "o_order_log00.csv";
    const writableStream = fs.createWriteStream(filename);

    let total;
    db.each(`select SUM(COUNT) AS TOTAL from orders `, (error, row) => {
      if (error) {
        return console.log(error.message);
      }
      total = row["TOTAL"];
    }).wait();
    db.each(
      `select PRODUCT_NAME , SUM(COUNT) AS SUM   from orders group by product_name`,
      (error, row) => {
        if (error) {
          return console.log(error.message);
        }
        row["SUM"] = row["SUM"] / total;
        stringifier.write(row);
      }
    );

    const stringifier = stringify();
    stringifier.pipe(writableStream);
  })
  .on("end", function order1() {
    const filename = "1_order_log00.csv";
    const writableStream = fs.createWriteStream(filename);

    db.each(
      `SELECT PRODUCT_NAME , MAX(SUM) AS MAX , BRAND_NAME FROM (select PRODUCT_NAME , SUM(COUNT) AS SUM , BRAND_NAME  from ORDERS group by PRODUCT_NAME , BRAND_NAME) GROUP BY PRODUCT_NAME`,
      (error, row) => {
        if (error) {
          return console.log(error.message);
        }
        stringifier.write({
          product_name: row["PRODUCT_NAME"],
          brand_name: row["BRAND_NAME"],
        });
      }
    );

    const stringifier = stringify();
    stringifier.pipe(writableStream);
  });
