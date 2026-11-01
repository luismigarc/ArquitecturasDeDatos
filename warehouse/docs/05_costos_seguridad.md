# Estimación de costos y controles de seguridad

## Costos (us-east-1, precios de lista aproximados; confirmar en https://calculator.aws)

| Servicio | Configuración | Precio aproximado | Mientras está encendido |
|---|---|---|---|
| EC2 | t3.medium + 30 GB gp3 | US$0,042/h + US$0,08/GB-mes | ~US$1,0/día |
| RDS for MySQL | db.t3.micro, 20 GB gp3 | US$0,017/h (capa gratuita 750 h/mes el primer año) | ~US$0,4/día |
| DocumentDB | 1 × db.t3.medium + almacenamiento e I/O | US$0,078/h (+ US$0,10/GB-mes, US$0,20/millón de I/O) | ~US$1,9/día |
| Redshift Serverless | 8 RPU base | US$0,375 por RPU-hora, sólo mientras ejecuta consultas (cobro por segundo, mínimo 60 s) | una corrida completa del DAG ≈ 3-5 min activos ≈ **US$0,15-0,25**; sin consultas = US$0 de cómputo |
| S3 | < 50 MB | US$0,023/GB-mes + solicitudes | centavos |
| SSM Parameter Store | 5 parámetros estándar | gratis | — |
| QuickSight | 1 autor | prueba gratis 30 días; luego por autor/mes | cancelar al terminar |
| **Total** | | | **≈ US$3,3/día encendido + ~US$0,2 por corrida del DAG** |

Créditos frecuentes en cuentas nuevas: **US$300 de prueba de Redshift Serverless**, capa gratuita de RDS,
prueba de QuickSight y, en algunos casos, prueba de DocumentDB.

**Controles de costo implementados:**
* `pausar.sh` detiene EC2, RDS y DocumentDB entre sesiones (sólo queda el almacenamiento).
* Redshift Serverless con la capacidad mínima (8 RPU) y **límite de uso de 10 RPU-hora por día**
  que desactiva el workgroup si se supera (máximo ~US$3,75/día de cómputo).
* El DAG no tiene programación automática (se ejecuta a demanda), así Redshift no se usa sin necesidad.
* Ciclo de vida en S3 para versiones antiguas (30 días).
* `99_destroy.sh` elimina todo al final.

## Controles de seguridad implementados

| Control | Dónde |
|---|---|
| Bucket privado: Block Public Access, política que niega tráfico sin TLS, cifrado SSE-S3, versionado | `01_red_s3.sh` |
| Sin secretos en el repositorio ni en el user data: contraseñas aleatorias en **SSM Parameter Store (SecureString, cifradas con KMS)**; la EC2 las lee con su rol | `02_iam_secretos.sh`, `user_data.sh.tpl` |
| **Mínimo privilegio**: rol de la EC2 limitado al bucket, a `/tallerdw/*` y a refrescar QuickSight; rol de Redshift sólo con lectura de `raw/` | `02_iam_secretos.sh` |
| Redshift lee S3 con **rol IAM** (no llaves en el `COPY`) | `04_redshift.sh`, `load.py` |
| RDS y DocumentDB **no públicos**, sólo accesibles desde el SG de la EC2 | `01_red_s3.sh`, `03_bases.sh` |
| Redshift: puerto 5439 sólo desde la EC2/VPC y el rango de IPs de QuickSight | `01_red_s3.sh` |
| Airflow (8080) y SSH (22) sólo desde la IP del operador /32 | `01_red_s3.sh`, `reanudar.sh` |
| **Cifrado en tránsito**: TLS a DocumentDB (obligatorio), a RDS (bundle de AWS) y a Redshift (`sslmode=require`) | `connections.py`, `profiles.yml` |
| **Cifrado en reposo**: RDS, DocumentDB, EBS, S3 y Redshift cifrados | scripts 01-06 |
| **IMDSv2 obligatorio** en la EC2 | `06_ec2.sh` |
| Usuario de BI de **sólo lectura** sobre `marts` para QuickSight | `tools/redshift_reporting_user.py` |
| `.gitignore` excluye `.env`, llaves `.pem` y `.state.env` | raíz |

**Mejoras para producción (fuera del alcance del taller):** usuario ETL propio en MySQL en vez del maestro,
rotación de contraseñas con Secrets Manager, EC2 en subred privada con acceso por Session Manager,
QuickSight Enterprise con conexión VPC (Redshift sin acceso público), backups y Multi-AZ.
