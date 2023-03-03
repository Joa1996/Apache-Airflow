import pandas as pd

df1 = pd.DataFrame(
    {
        "ID": ["1", "2", "3", "4","5"],
        "Nombre": ["Juan", "Carmen", "Sofia", "Daniel","Manuela"],
        "Apellido": ["Perez", "Sosa", "Bianchi", "Pereyra","Pellegrini"],
    },
)


df2 = pd.DataFrame(
   {
        "ID": ["1", "2", "3", "4","5"],
        "Deuda": ["70000", "85000", "23000", "120000","234"],
    },
)

df3 = pd.DataFrame(
   {
        "ID": ["1", "2","2", "3", "4","5"],
        "Deuda": ["70000", "85000","34500", "23000", "120000","234"],
        "Apellido": ["Perez", "Sosa","Sosa", "Bianchi", "Pereyra","Perez"]
    },
)

df4 = pd.DataFrame(
   {
        "ID": ["1", "2", "3", "4","5"],
        "Deuda": ["70000", "85000", "23000", "120000","234"],
        "Ape": ["Perez", "Sosa", "Bianchi", "Pereyra","Pellegrini"]
    },
)

### JOIN
inner=pd.merge(df1,df2) #Por defecto Merge realiza un Inner
'''
Por defecto merge busca las columnas que tienen el mismo nombre y el mismo valor, en base
a eso realiza el JOIN
'''
inner2=pd.merge(df1,df3,on='ID')#De esta manera le decimos que haga el JOIN por el campo que nosotros queremos que lo haga, con la opcion ON

inner3=pd.merge(df1,df3,on=['ID','Apellido'])#De esta manera le decimos que haga un JOIN teniendo en cuenta el ID y el apellido

inner4=pd.merge(df1,df3,on=['ID','Apellido'],how='left') #Con el argumento how especificamos el tipo de join que queremos, (Inner,Left,Right,Full Outer)

inner5=pd.merge(df1,df4,how='inner',left_on='Apellido',right_on='Ape') #Con left_on y right_on podemos hacer un join por un campo con nombres distintos en cada dataframe

_axis=pd.concat([df1,df2],axis=1) #De esta manera podemos concatenar dataframes como si fueran un Join

print(inner5)

##  Append

append=pd.concat([df1,df2]) #De esta manera anexamos dos dataframes

append2=pd.concat([df1,df2,df3]) #Anexar la cantidad de dataframes que queramos 

append3=pd.concat([df1,df2],axis=0) #De esta manera tambien  podemos anexar dataframes como si fueran un Join

print(append3)

'''
Combinar de lado a lado usar merge o join (axis=1)--> Merge
Anexar usar (axis=0)--> Concat

Video=https://www.youtube.com/watch?v=wzN1UyfRSWI
'''

