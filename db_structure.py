from sqlalchemy.orm import declarative_base, relationship, Session
from sqlalchemy import create_engine, text
from sqlalchemy import select
from sqlalchemy import Column, Float, ForeignKey, Integer, String, DateTime
import pandas as pd

Base = declarative_base()

class Producto(Base):
    __tablename__ = "producto"
    id = Column(Integer, primary_key = True)
    nombre = Column(String(20), nullable = False, unique = True)
    cantidad = Column(Integer, nullable = False)
    tipo = Column(String(11), nullable = False)

    def get_recetas(self, session):
        recetas = session.execute(select(ProductoReceta).filter_by(id_producto = self.id))
        return [session.execute(select(Receta).filter_by(id = receta[0].id_receta)).scalar_one() for receta in recetas]

    def __repr__(self):
        return str(self.id) + "-" + self.nombre + "#" + str(self.cantidad) + "/" + self.tipo
    
class Receta(Base):
    __tablename__ = "receta"
    id = Column(Integer, primary_key = True)
    nombre = Column(String(20), nullable = False, unique = True)
    tiempo = Column(Integer, nullable = False)

    productos = relationship("ProductoReceta")

    def __repr__(self):
        return str(self.id) + "-" + self.nombre + "#" + str(self.tiempo)

class Compra(Base):
    __tablename__ = "compra"
    id = Column(Integer, primary_key = True)
    fecha = Column(DateTime, nullable = False)

    productos = relationship("ProductoCompra")

    def __repr__(self):
        return str(self.id) + "-" + self.fecha

class ProductoReceta(Base):
    __tablename__ = "producto_receta"
    id_producto = Column(Integer, ForeignKey(Producto.id), primary_key = True)
    id_receta = Column(Integer, ForeignKey(Receta.id), primary_key = True)
    cantidad = Column(Integer, nullable = False)

class ProductoCompra(Base):
    __tablename__ = "producto_compra"
    id_producto = Column(Integer, ForeignKey(Producto.id), primary_key = True)
    id_compra = Column(Integer, ForeignKey(Compra.id), primary_key = True)
    precio = Column(Float, nullable = False)
    cantidad = Column(Integer, nullable = False)
    caduca = Column(DateTime)

engine = create_engine("sqlite+pysqlite:///:memory:")
Base.metadata.create_all(engine)

# volcado de datos
with Session(engine) as session:
    session.execute(text("INSERT INTO producto VALUES (1, 'huevo', 5, 'frigorifico')"))
    session.execute(text("INSERT INTO producto VALUES (2, 'harina', 1000, 'despensa')"))
    session.execute(text("INSERT INTO producto VALUES (3, 'azucar', 1000, 'despensa')"))
    session.execute(text("INSERT INTO producto VALUES (4, 'alubias', 500, 'congelador')"))
    session.execute(text("INSERT INTO receta VALUES (1, 'bizcocho', 50)"))
    session.execute(text("INSERT INTO compra VALUES (1, '2022-06-21')"))
    session.execute(text("INSERT INTO compra VALUES (2, '2022-06-21')"))
    session.execute(text("INSERT INTO producto_receta VALUES (1, 1, 2)"))
    session.execute(text("INSERT INTO producto_receta VALUES (2, 1, 750)"))
    session.execute(text("INSERT INTO producto_receta VALUES (3, 1, 500)"))
    session.execute(text("INSERT INTO producto_compra VALUES (1, 1, 3.75, 3, '2022-06-25')"))
    session.execute(text("INSERT INTO producto_compra VALUES (2, 2, 1.50, 500, null)"))
    session.execute(text("INSERT INTO producto_compra VALUES (3, 1, 2, 1000, '2023-06-21')"))
    session.commit()

# consulta de los datos usando CORE API
with Session(engine) as session:
    result = session.execute(text("SELECT * FROM producto"))
    print(result.all())

# inserción usando ORM
with Session(engine) as session:
    # estado: nuevo
    nuevo_producto = Producto(nombre = "tomate", cantidad = 3, tipo = "frigorifico")
    nueva_receta = Receta(nombre = 'tomate frito', tiempo = 10)

    # estado: manejado
    session.add(nuevo_producto)
    session.add(nueva_receta)

    # actualizamos
    session.flush()

    # podemos acceder a esta información
    print(nuevo_producto.id, nueva_receta.id)

# actualizacion de registros usando ORM
with Session(engine) as session:
    # recuperamos el objeto
    resultado = session.execute(select(Producto).filter_by(nombre = "huevo"))
    huevo = list(resultado)[0][0]
    print(type(huevo), huevo)

    # lo modificamos
    huevo.cantidad += 1

    # ahora es un objeto con cambios no confirmados
    print(session.dirty)

    # confirmamos los cambios
    session.flush()

    # ahora ya está confirmado
    print(session.dirty)

# actualizacion de registros usando ORM
with Session(engine) as session:
    receta = session.get(Receta, 1)
    producto_receta1 = session.get(ProductoReceta, (1, 1))
    producto_receta2 = session.get(ProductoReceta, (2, 1))
    producto_receta3 = session.get(ProductoReceta, (3, 1))
    print(receta)

    session.delete(producto_receta1)
    session.delete(producto_receta2)
    session.delete(producto_receta3)
    session.delete(receta)

    session.flush()

    receta = session.get(Receta, 1)
    print(receta)

    session.rollback()

with Session(engine) as session:
    producto = session.get(Producto, 1)
    print(producto)

    recetas = producto.get_recetas(session)
    print(recetas)