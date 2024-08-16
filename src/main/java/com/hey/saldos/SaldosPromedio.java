package com.hey.saldos;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.TreeMap;

public class SaldosPromedio {
	
	public Connection conexion;
	
	public SaldosPromedio() {
		try {
		    conexion = DriverManager.getConnection("jdbc:h2:mem:test");
		}catch(SQLException err) {
			err.printStackTrace();
		}
	}
	
	public void createTable() {
		StringBuilder qry = new StringBuilder(); 
		qry.
		append("Create table movimientos(").
		append("mov_fecha date,").
		append("mov_natura varchar(10),").
		append("mov_cantidad double,").
		append("mov_descri varchar(100))");
        
		try {
			Statement statement = conexion.createStatement();
	        	statement.execute(qry.toString());
	        	System.out.println("Se creo la tabla movimientos");
		}catch(SQLException err) {
			err.printStackTrace();
		}
	}
	
	public void loadData() {
		StringBuilder qry = new StringBuilder(); 
		qry.
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-01',").append("'Abono',").append("25000.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-01',").append("'Abono',").append("20.30,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-01',").append("'Cargo',").append("6000.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-02',").append("'Abono',").append("30000.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-02',").append("'Abono',").append("25.60,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-03',").append("'Abono',").append("35000.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-03',").append("'Abono',").append("14.52,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-03',").append("'Cargo',").append("25.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-08',").append("'Abono',").append("26000.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-08',").append("'Abono',").append("24.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-09',").append("'Cargo',").append("36000.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-09',").append("'Cargo',").append("6530.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-15',").append("'Cargo',").append("2500.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-15',").append("'Cargo',").append("350.65,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-16',").append("'Abono',").append("50000.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-16',").append("'Abono',").append("35.60,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-22',").append("'Cargo',").append("12.30,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-22',").append("'Abono',").append("65000.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-22',").append("'Abono',").append("26.35,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-23',").append("'Cargo',").append("13.60,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-23',").append("'Cargo',").append("19560.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-29',").append("'Abono',").append("25000.00,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-29',").append("'Abono',").append("15.60,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-30',").append("'Cargo',").append("1.50,").append("'-');").
		append("insert into movimientos(mov_fecha, mov_natura, mov_cantidad,mov_descri)").
		append(" values(").append("'2009-06-30',").append("'Cargo',").append("24959.00,").append("'-');");
		
		try {
			Statement statement = conexion.createStatement();
		        statement.execute(qry.toString());
	        	System.out.println("Se cargo la tabla movimientos");
		}catch(SQLException err) {
			err.printStackTrace();
		}
	}
	
	public void queryData() {
		StringBuilder qry = new StringBuilder(); 
		qry.
		append("select * from movimientos");
        
		try {
			Statement statement = conexion.createStatement();
	         
			ResultSet rs = statement.executeQuery(qry.toString());
			
			while (rs.next()) {
                		System.out.print(rs.getString("mov_fecha") + "\t");
                		System.out.print(rs.getString("mov_natura") + "\t");
                		System.out.print(String.format("%,.2f", rs.getDouble("mov_cantidad")));
                		System.out.println("");
            		}
			statement.close();
			rs.close();
		}catch(SQLException err) {
			err.printStackTrace();
		}
	}
	
	public void calcular(String fecha_ini, String fecha_fin) {
		try {
			Statement sentencia = conexion.createStatement(); 
			
			StringBuilder qry = new StringBuilder(); 
			/*
			qry.
			append("select mov_fecha, avg(abono - cargo)saldo_prom from( ").
			append("select mov_fecha, sum(mov_cantidad)abono ").
			append("  from movimientos ").
			append(" where mov_fecha >= '").append(fecha_ini).append("'").
		    append("   and mov_fecha <= '").append(fecha_fin).append("'").
		    append("   and mov_natura = 'Abono'").
		    append(" order by mov_fecha").
		    append(" group by mov_fecha").
			append("union").
		    append("select sum(mov_cantidad)cargo ").
		    append("  from movimientos ").
		    append(" where mov_fecha >= '").append(fecha_ini).append("'").
		    append("   and mov_fecha <= '").append(fecha_fin).append("'").
		    append("   and mov_natura = 'Cargo'").
		    append(" order by mov_fecha").
		    append(" group by mov_fecha").
		    append(")");*/
			
			//Creo que esta funcionaria mejor:
			qry.
			append("select mov_fecha, AVG(saldo_diario) AS saldo_diario").
			append(" from (").
			append("    select mov_fecha, ").
			append("           SUM(").
			append("        	   CASE WHEN mov_natura = 'Abono'").
			append("               THEN mov_cantidad").
			append("               ELSE -mov_cantidad").
			append("               END").
			append("           ) AS saldo_diario").
			append("    from movimientos").
			append("   where mov_fecha >= '").append(fecha_ini).append("'").
		    append("     and mov_fecha <= '").append(fecha_fin).append("'").
			append("    GROUP BY mov_fecha").
			append(") GROUP BY mov_fecha");
			
			ResultSet rs = sentencia.executeQuery(qry.toString());
			
			double account = 0d;
			Map<String, Double> map = new TreeMap<>();
			rs.next();
			for(int day = 1; day <= 30; day++){
				String fx = "2009-06-" + (day < 10?"0"+day:day);
				if(fx.equals(rs.getString("mov_fecha"))) {
					account += rs.getDouble("saldo_diario");
					map.put(fx, account);
					rs.next();
				} else {
					map.put(fx, account);
				}
            }
			sentencia.close();
			rs.close();
			double acum = 0d;
			for (Map.Entry<String, Double> entry : map.entrySet()) {
				double sd = entry.getValue();
				acum += sd;
				System.out.print(entry.getKey() + "\t");
	            System.out.print(String.format("%,.2f", sd) + "\t");
	            System.out.print("Saldo diario");
	            System.out.println("");
			}
			System.out.print("\t");
            System.out.print(String.format("%,.2f", acum) + "\t");
            System.out.println("Suma Saldos diarios");
            
            System.out.print("\t");
            System.out.print(String.format("%,.2f", acum/map.size()) + "\t");
            System.out.println("Promedio diario");
		}catch(SQLException err) {
			err.printStackTrace();
		}
	}
	public static void main(String[] args) {
		SaldosPromedio sp = new SaldosPromedio();
		sp.createTable();
		sp.loadData();
		sp.queryData();
		sp.calcular("2009-06-01", "2009-06-30");
	}

}
