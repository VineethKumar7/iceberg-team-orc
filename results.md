# Results
from x86_64 Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz (ubuntu 20.04.1)

(**not** avg running)

| #                                | query3 | query6 |
|----------------------------------|--------|--------|
| duckdb with no processing at all | 0.05   | 0.0005 |
| using iceberg and orc format     | 9.6    | 1.46   |
| Physical Optimisation1 - sorting |        |        |
| Physical Optimisation2 -         |        |        |