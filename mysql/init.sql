-- Exécuté une seule fois, au premier démarrage du conteneur mysql (volume vide).
CREATE TABLE IF NOT EXISTS nifi_test.cars (
  Car          VARCHAR(200) NOT NULL,
  MPG          DOUBLE       NOT NULL,
  Cylinders    INT          NOT NULL,
  Displacement DOUBLE       NOT NULL,
  Horsepower   INT          NULL,
  Weight       INT          NOT NULL,
  Acceleration DOUBLE       NOT NULL,
  Model        INT          NOT NULL,
  Origin       VARCHAR(10)  NOT NULL
);
