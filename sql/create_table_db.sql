CREATE TABLE IF NOT EXISTS cota_parlamentar (
            ID INT GENERATED BY DEFAULT AS IDENTITY,
            numano INTEGER,
            datemissao TIMESTAMP,
            sgpartido VARCHAR(18),
            sguf VARCHAR(2),
            txtdescricao VARCHAR(127),
            vlrliquido DECIMAL(8,2),
            vlrrestituicao DECIMAL(8,2),
            nucarteiraparlamentar INTEGER,
            txttrecho VARCHAR(127)
            )