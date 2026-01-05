install.packages("RSQLite")
install.packages("tidyverse")
library(RSQLite)
library(tidyverse)
# Crear la conexión a la base de datos (se crea el archivo físicamente)
con <- dbConnect(RSQLite::SQLite(), "Olist_DB.sqlite")

# Leer un ejemplo (Clientes) y guardarlo en la DB
customers <- read.csv("olist_customers_dataset.csv")
dbWriteTable(con, "customers", customers, overwrite = TRUE)

# Verifica que la tabla existe
dbListTables(con)

# Cerrar conexión
dbDisconnect(con)
library(RSQLite)
library(DBI)
con <- dbConnect(RSQLite::SQLite(), "Olist_DB.sqlite")
print(con)

library(RSQLite)
library(DBI)
con <- dbConnect(RSQLite::SQLite(), "Olist_DB.sqlite")

# Buscamos los archivos
archivos <- list.files(pattern = "*.csv")

# Los metemos a la base de datos
for (archivo in archivos) {
  nombre_tabla <- gsub("olist_|_dataset|.csv", "", archivo)
  temp_df <- read.csv(archivo)
  dbWriteTable(con, nombre_tabla, temp_df, overwrite = TRUE)
  message(paste("✓ Cargada tabla:", nombre_tabla))
}

# Verificamos que funcionó
dbListTables(con)

dbDisconnect(con)
library(RSQLite)
library(DBI)
con <- dbConnect(RSQLite::SQLite(), "Olist_DB.sqlite")
dbListTables(con)
library(RSQLite)
library(DBI)

library(RSQLite)
library(DBI)

# 1. Asegurar conexión (Abrir la puerta)
con <- dbConnect(RSQLite::SQLite(), "olist_DB.sqlite")

# 2. Listar SOLO archivos CSV reales (evitamos carpetas)
archivos <- list.files(path = "datos_csv", pattern = "\\.csv$", full.names = TRUE)

# Verificación: Debería decir "Archivos encontrados: 9" (o el número de CSVs que tengas)
print(paste("Archivos encontrados:", length(archivos)))

# 3. Cargar los datos correctamente
if(length(archivos) > 0) {
  for (archivo in archivos) {
    # Limpiamos el nombre para la tabla
    nombre_tabla <- gsub("olist_|_dataset|\\.csv|datos_csv/", "", archivo)
    
    # Leemos y guardamos
    temp_df <- read.csv(archivo)
    dbWriteTable(con, nombre_tabla, temp_df, overwrite = TRUE)
    message(paste("✓ Tabla cargada:", nombre_tabla))
  }
}

# 4. Verificación final
dbListTables(con)
# 1. Traer la tabla de pedidos a R
orders <- dbReadTable(con, "orders")

# 2. Convertir texto a formato de fecha real (POSIXct)
# Esto permite que Power BI entienda qué día y hora es cada compra
orders$order_purchase_timestamp <- as.POSIXct(orders$order_purchase_timestamp, format="%Y-%m-%d %H:%M:%S")
orders$order_delivered_customer_date <- as.POSIXct(orders$order_delivered_customer_date, format="%Y-%m-%d %H:%M:%S")

# 3. Limpieza de Nulos (Opcional pero recomendado)
# Eliminamos filas donde no hay fecha de entrega (pedidos cancelados o en camino)
orders_clean <- orders[!is.na(orders$order_delivered_customer_date), ]

# 4. Guardar la nueva tabla limpia en SQLite
dbWriteTable(con, "orders_processed", orders_clean, overwrite = TRUE)

message("✅ Tabla 'orders_processed' creada con éxito.")

# Consulta SQL para unir las tablas principales
query_unificada <- "
SELECT 
    o.order_id, 
    o.order_purchase_timestamp, 
    o.customer_id,
    i.price, 
    i.freight_value,
    p.product_category_name
FROM orders_processed o
JOIN order_items i ON o.order_id = i.order_id
JOIN products p ON i.product_id = p.product_id
"

# Ejecutar y guardar el resultado como una nueva tabla en la DB
tabla_reporte <- dbGetQuery(con, query_unificada)
dbWriteTable(con, "reporte_ventas_powerbi", tabla_reporte, overwrite = TRUE)

# Verificamos que ahora tenemos 11 tablas (las 9 originales + 2 limpias)
dbListTables(con)
dbDisconnect(con)
library(RSQLite)
library(DBI)

# Conectamos asegurándonos de estar en la carpeta del proyecto
con <- dbConnect(RSQLite::SQLite(), "olist_DB.sqlite")

# VERIFICACIÓN IMPORTANTE: 
# Si esto sale vacío (character(0)), es que R no encuentra el archivo en esta carpeta.
dbListTables(con)
# 1. Forzamos a R a ir a tu carpeta de proyecto
setwd("~/PAULO/Proyecto_Enero-02")

# 2. Conectamos (ahora sí debería encontrar el archivo)
library(RSQLite)
con <- dbConnect(RSQLite::SQLite(), "olist_DB.sqlite")

# 3. VERIFICACIÓN CRÍTICA: 
# Si esto vuelve a salir vacío, detente y dime. 
# Si salen las tablas, ¡ya ganamos!
dbListTables(con)
# 1. Leemos la tabla maestra que ya confirmamos que existe
tabla_final <- dbReadTable(con, "reporte_ventas_powerbi")

# 2. La guardamos como un CSV en tu carpeta de proyecto
write.csv(tabla_final, "Olist_Final_Para_PowerBI.csv", row.names = FALSE)

# 3. Cerramos la conexión de forma segura
dbDisconnect(con)
message("🚀 ¡LOGRADO! El archivo 'Olist_Final_Para_PowerBI.csv' está listo en tu carpeta.")
# 1. Volvemos a conectar
library(RSQLite)
con <- dbConnect(RSQLite::SQLite(), "olist_DB.sqlite")

# 2. Consulta SQL para unir Ventas + Clientes (para obtener el Estado/City)
query_mapa <- "
SELECT 
    v.*, 
    c.customer_state,
    c.customer_city
FROM reporte_ventas_powerbi v
JOIN customers c ON v.customer_id = c.customer_id
"

tabla_mapa <- dbGetQuery(con, query_mapa)

# 3. Exportamos la versión mejorada
write.csv(tabla_mapa, "Olist_Final_Mapa.csv", row.names = FALSE)
dbDisconnect(con)

message("🚀 ¡Listo! Carga 'Olist_Final_Mapa.csv' en Power BI.")