##'Practica Airbnb                    ##
##'Master de Ciencia de Datos         ##
##'Extracción, Transformación y Carga ##
##'Preparado por Gozde Yazganoglu     ##
########################################
########################################


library(tidyverse)
library(dbplyr)
library(RSQLite)

#Primero creamos la connección

conn <- DBI::dbConnect(RSQLite::SQLite(), "../data/hw/airbnb.sqlite")

DBI::dbListTables(conn)# Para mirar a las tablas.

#'Extracción
#'1. Extracción (listings) Crea un data frame de pandas o de R a partir de la tabla listings
#'con las consideraciones que se indican a continuación. Con SQL, haz un join con la tabla
#'hoods para añadir el dato de distrito (neighbourhood_group) y asegúrate de que extraes
#'esta columna en el data frame en lugar de neighbourhood. Para saber qué columnas
#'necesitas, tendrás que seguir leyendo la práctica para ver qué es lo que irás necesitando.
#'


join_1 <- "SELECT  l.name, l.description, l.host_id,  
h.neighbourhood_group as neighbourhood, l.room_type,  
l.price, l.number_of_reviews, l.review_scores_rating
FROM listings as l JOIN hoods as h
WHERE l.neighbourhood_cleansed = h.neighbourhood"

listings <- collect(tbl(conn, sql(join_1)))


#'2. Extracción (reviews) Descarga en un data frame de pandas o de R la información
#'necesaria de la tabla reviews con las siguientes consideraciones:
#'  • Con SQL, haz un join con la tabla hoods para añadir el dato de distrito
#'(neighbourhood_group) y asegúrate de que extraes esta columna en el data frame
#'en lugar de neighbourhood.
#'• También en SQL, cuenta a nivel de distrito y mes el número de reviews. Para
#'calcular el mes a partir de una fecha en una tabla SQL, usa 
#'strftime('%Y-%m', date) as mes.
#'• Además, extrae los datos desde 2011 en adelante (también SQL). Te resultará de
#'nuevo útil la función strftime. Observación. Esta función devuelve un texto.


# Primero hacemos el join con el tabla hoods.

join_2 <- "SELECT COUNT(r.id) as review_count, r.listing_id, h.neighbourhood_group as districto, l.number_of_reviews, STRFTIME('%Y-%m', r.date) as mes, r.comments
FROM reviews AS r
LEFT JOIN listings AS l ON l.id = r.listing_id
LEFT JOIN hoods AS h ON l.neighbourhood_cleansed = h.neighbourhood
WHERE mes > '2010-12'
GROUP BY mes, districto, l.number_of_reviews"

reviews<- collect(tbl(conn, sql(join_2)))

#'3. Transformación (listings). Antes de realizar la agregación que se pide, tienes que
#'tratar las columnas price,, number_of_reviews y review_scores_rating. Empieza
#'con el precio (de las otras dos columnas te encargarás en el siguiente ejercicio). Necesitas
#'pasarla a numérica. Ahora mismo es de tipo texto y lo primero que necesitamos es quitar
#'símbolos raros. Tanto R como Python sabe convertir un texto como "15.00" a número,
#'pero no saben convertir "$1,400.00". Tienes que quitar tanto el símbolo del dólar
#'como la coma. En expresiones regulares, el símbolo del dólar se usa para una cosa muy
#'concreta, así que necesitarás usar algo como "\\$" (lo que se conoce como escapar).

listings<-listings%>%
  mutate(price = str_replace_all(price, ",", ""),# para nosotros "," no es necessario
         price = str_remove_all(price , "\\$"),#Hay que quitar "$" para cambiarse a numerico
         price = as.numeric(price)) # se convertimos a numerico

#'4. Transformación (listings). Toca imputar los valores missing de number_of_reviews
#'y review_scores_rating. Normalmente en estos casos se habla con la gente que más
#'usa los datos y se llega con ellos a un acuerdo de cómo se imputaría esta informa-
#'  ción. En este caso, imputa los valores missing con valores reales dentro de la tabla,
#'a nivel de room_type, escogidos de manera aleatoria. Es decir, si hay un valor miss-
#'  ing en number_of_reviews para un registro con room_type == "Entire home/apt", lo
#'reemplazarías con un valor aleatorio de esa misma columna para los que room_type sea
#'"Entire home/apt". Tienes libertad para plantear esto como te resulte más cómodo.
#'Pista. Yo he hecho un bucle for() con R base (sí, lo nunca visto en mí :P)

# Buscamos todo las filas para encontrar NA
for (i in 1:nrow(listings)) {
    # Si encontramos NA...
    if (is.na(listings$number_of_reviews[[i]])) {
      # miramos cual sera el room_type
      rt <- listings$room_type[[i]]
      # Imputar valor aleatorio correspondiente a cada NA que tendra un room_type similar
        listings$number_of_reviews[[i]] <- sample(na.omit(listings$number_of_reviews[listings$room_type==rt]),size=1)
    }
  }


#Repetimos mismo bucle para review_scores_rating.
for (i in 1:nrow(listings)) {
  if (is.na(listings$review_scores_rating[[i]])) {
    rt <- listings$room_type[[i]]
    listings$review_scores_rating[[i]] <- 
      sample(na.omit(listings$review_scores_rating[listings$room_type==rt]),size=1)
  }
}

#'5. Transformación (listings). Con los missing imputados y el precio en formato numérico
#'ya puedes agregar los datos. A nivel de distrito y de tipo de alojamiento, hay que calcular:
#'  • Nota media ponderada (review_scores_rating ponderado con number_of_reviews).
#'  • Precio mediano (price).
#'  • Número de alojamientos (id).
#'La tabla resultante tendrá cuatro columnas: distrito (llamada habitualmente neighbourhood_group),
#'tipo de alojamiento (room_type), nota media y precio mediano. Esta tabla puede ser útil
#'para estudiar diferencias entre mismo un tipo de alojamiento en función del distrito en el que
#'esté.

#Este coresponde uno de las 2 tablas objetivos, la segunda 


tabla_objectivo2 <- listings %>%
  group_by(neighbourhood, room_type) %>% 
  summarize(media_ponderada_rating = weighted.mean(review_scores_rating, number_of_reviews),
            precio_mediano = median(price))

#'6. Transformación (reviews). La mayor parte de la transformación para Reviews la has
#'hecho ya con SQL. Vamos a añadir ahora a simular que tenemos un modelo predictivo
#'y lo vamos a aplicar sobre nuestros datos. Así, la tabla que subamos de nuevo a la
#'base de datos tendrá la predicción añadida. El último mes disponible es julio, así que
#'daremos la predicción para agosto. Esto no es una asignatura de predicción de series
#'temporales, así que nos vamos a conformar con tomar el valor de julio como predicción
#'para agosto (a nivel de distrito). Es decir, si el dato en "Centro" para julio es de 888
#'reviews, añadiremos una fila con los valores "Centro", "2021-08" y 888, así para cada
#'distrito. Tienes libertad para plantearlo como veas adecuado. Al final, deja el data
#'frame ordenado a nivel de distrito y mes. Pista. Yo he creado un data frame
#'nuevo con todas estas predicciones y lo he apilado al data frame original. Esto se puede
#'hacer con la función bind_rows() de dplyr o el método append() o función concat()
#'de pandas.
#'

reviews_august2021<-reviews%>%
  filter(mes=="2021-07")%>%
  mutate(mes= "2021-08")


#tenemos un dataframe con predicciones creados.

reviews<-bind_rows(reviews, reviews_august2021)

#'7. (este es lío) Transformación (reviews). Hay casos que no tienen dato, por ejemplo,
#'febrero de 2011 en Arganzuela. Como no hay dato, asumiremos que es 0. Siguiendo
#'esta idea, añade todos los registros necesarios a la tabla. Puedes hacerlo de la manera
#'que te resulte más intuitiva. Recuerda ordenar la tabla final por distrito y mes.
#'Pista. Yo he creado primero un vector con todas las fechas posibles y otro con los
#'posibles distritos. Con esos vectores hago un data frame de dos columnas, con todas las
#'combinaciones posibles entre meses y distritos. Hay muchas formas de hacer eso. Luego
#'hago un full join con los datos originales. Si después del join la columna reviews tiene
#'valor missing, es que no estaba en el caso original. Sustituyo esos missing por ceros y ya
#'tengo la tabla final.


df_aux<-tibble(expand.grid(mes=c(unique(reviews$mes)),
                           districto= c(unique(reviews$districto))))


#join de ambos df para añadir combinaciones que faltaban
tabla_objectivo1 <- df_aux%>%
  full_join(y= reviews, by = c("mes", "districto"))%>%
  group_by(mes, districto, review_count)%>%
  mutate(review_count = ifelse(is.na(review_count), 0, review_count),
         listing_id = ifelse(is.na(listing_id), 0, listing_id),
         number_of_reviews = ifelse(is.na(number_of_reviews), 0, number_of_reviews),
         )%>%
  arrange(mes,districto)
#ya tenemos un data frame sin nulos.
glimpse(tabla_objectivo1) 


#'8. Carga. Sube a la base de datos las dos tablas que has creado. No sobreescibas las
#'que hay: crea dos tablas nuevas. Haz una prueba de que todo está en orden, haciendo
#'SELECT * FROM nombre_tabla LIMIT 10 para cada tabla. Si la fecha tiene un formato
#'raro, es posible que necesites definirla en el data frame como tipo texto

#primero creamos las tablas en el database

dbWriteTable(conn = conn, 
             name = "listings_manipulated",
             value = tabla_objectivo2)

dbWriteTable(conn = conn, 
             name = "reviews_manipulated",
             value = tabla_objectivo1)

#luego consultamos a database. no hay que crear otro dataset y no lo creamos.

DBI::dbListTables(conn)# Para mirar a las tablas y database.

glimpse_listing_manipulated <- "SELECT * FROM listings_manipulated LIMIT 10"

collect(tbl(conn, sql(glimpse_listing_manipulated)))

glimpse_reviews_manipulated <- "SELECT * FROM reviews_manipulated LIMIT 10"

collect(tbl(conn, sql(glimpse_reviews_manipulated)))
