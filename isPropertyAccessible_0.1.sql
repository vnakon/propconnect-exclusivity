
CREATE OR REPLACE FUNCTION isPropertyAccessible(property_in integer, company_in integer) RETURNS integer AS $$
	DECLARE passed INTEGER;
        BEGIN
        
		--------------------------------------------

		-- creating temporary dataset as input_graph_current structure

		DROP TABLE IF EXISTS input_graph_current;
		
		CREATE TABLE input_graph_current as
		SELECT  
			  row_number() OVER(ORDER BY prop_items.id) AS nextval
			, prop_allocs.offering_company_id as source
			, prop_allocs.receiving_company_id as target
			, case when pg.exclusive = true then -1 else 1 end as strength
			, $1 as px 
			
		FROM property_grouping_properties pgp
		LEFT JOIN property_groupings pg ON pg.id = pgp.property_grouping_id
		LEFT JOIN allocation_items prop_items ON prop_items.property_grouping_id = pg.id
		LEFT JOIN allocations prop_allocs ON prop_allocs.id = prop_items.allocation_id

		WHERE
			pgp.property_id=$1
		ORDER BY prop_items.id ;

		--------------------------------------------

		-- update data into *input_graph_saved* from *input_graph_current* 

		CREATE TABLE input_graph_saved_temp as SELECT row_number() OVER(ORDER BY nextval_temp) AS nextval, source, target, strength, px FROM (
		SELECT row_number() OVER() AS nextval_temp, igs.*
		FROM input_graph_saved as igs
		INNER JOIN input_graph_current as igc
		ON igs.source=igc.source 
		AND igs.target=igc.target
		AND igs.strength=igc.strength
		AND igs.px=igc.px
		WHERE igc.px=$1 and igs.px=$1 
		
		UNION

		SELECT 100+row_number() OVER() AS nextval_temp, igc.*
		FROM input_graph_saved as igs
		RIGHT JOIN input_graph_current as igc
		ON igs.source=igc.source 
		AND igs.target=igc.target
		AND igs.strength=igc.strength
		AND igs.px=igc.px
		WHERE igs.source is null and igc.px=$1 
		) as tempx  
		ORDER BY nextval_temp;

		DELETE FROM input_graph_saved where px=$1 ;
		INSERT INTO input_graph_saved SELECT * FROM input_graph_saved_temp order by px, nextval;
		DROP TABLE input_graph_saved_temp;
		DROP TABLE IF EXISTS input_graph_current;
		--------------------------------------------

		-- cleaning input data-set for *property_in* based on Xclusivity rules
		
		DROP TABLE IF EXISTS input_graph_cleaned;

		CREATE TABLE input_graph_cleaned as
		SELECT
			 source
			,target
			, strength
			, px
		FROM (
		SELECT 
			 t1.source
			, t1.target
			, t1.strength
			, t2.source as pr_source
			, t2.target as pr_target
			, t2.strength as pr_strength
			, case when ((t2.strength=-1 and t1.source=t2.target) or t2.strength=1 or t2.strength is NULL) or (t1.source=t2.source and t1.strength=-1 and t2.strength=-1)then 1 else 0 end as active
			, t1.px
			, t1.nextval
		FROM (select * from input_graph_saved where px=$1 order by nextval) as t1 
		LEFT JOIN (select * from input_graph_saved where px=$1 order by nextval) as t2 
		ON t1.nextval-1= t2.nextval
	
		) as temp
		where active=1
		order by nextval;

		-------------------------------------------------

		DROP TABLE IF EXISTS input_graph_cleaned_cycle;

		create table input_graph_cleaned_cycle as 
		SELECT
		   t1.source
		 , t1.target
		 , t1.strength
		 , t1.px
		 , t1.nextval
		FROM (
		select row_number() OVER()  AS nextval, * from input_graph_cleaned where px=$1 ) as t1
		LEFT JOIN (
		select row_number() OVER()  AS nextval, * from input_graph_cleaned where px=$1 ) as t2
		ON t1.nextval+1=t2.nextval

		WHERE NOT (t1.source=t2.source and t2.strength=-1) or t2.source IS NULL  ORDER by t1.nextval ;


		----- update input_graph_saved, in order to keep track of removes case #6
		
		DELETE FROM input_graph_saved where px=$1 ;
		INSERT INTO input_graph_saved SELECT row_number() OVER()  AS nextval, source, target, strength, px FROM input_graph_cleaned_cycle order by px, nextval; 

		-------------------------------------------------

		-- doing search in graph source/target where target is *company_in*  

		WITH RECURSIVE graph AS (
		    SELECT source
			  ,target
			  ,',' || source::text || ',' || target::text || ',' AS path
			  ,1 AS depth
		    FROM   input_graph_cleaned_cycle
		    WHERE  source = 0

		    UNION ALL
		    SELECT o.source
			  ,o.target
			  ,g.path || o.target || ','
			  ,g.depth + 1
		    FROM   graph g
		    JOIN   input_graph_cleaned_cycle o ON o.source = g.target
		    WHERE  g.path !~~ ('%,' || o.source::text || ',' || o.target::text || ',%')
		    )
		    
		SELECT  count(*) INTO passed
		FROM    graph where target=$2;
		
		----------------------------------------------------
		--DROP TABLE IF EXISTS input_graph_cleaned;
		--DROP TABLE IF EXISTS input_graph_cleaned_cycle;
		----------------------------------------------------
		
                RETURN passed;
        END;
$$ LANGUAGE plpgsql;

------------ EXECUTE -------------------
-- first run for all property for any company, in order to clean history for the first run --

SELECT * FROM isPropertyAccessible(1,3);

-- test: + 01 level -> passed + 
-- test: + 02 level -> passed + 
-- test: + 03 level -> passed + 
-- test: + 04 level -> passed + 
-- test: + 05 level -> passed + 
-- test: + 06 level -> passed + 