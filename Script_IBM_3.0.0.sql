------------------------------------------------------------------------
--Version 3.0.0 => 25.11.2015
------------------------------------------------------------------------
-- Created by Francisco Riccio
------------------------------------------------------------------------
connect / as sysdba
COLUMN LOG NEW_VALUE snap_sid_fecha
SELECT INSTANCE_NAME||'_'||HOST_NAME||'_'||to_char(sysdate, 'YYYYMMDD_HH24MISS') LOG FROM V$INSTANCE;

spool ibm_3.0.0_&&snap_sid_fecha
prompt ######################################################################################################
prompt # Version 3.0.0 => 25.11.2015
prompt ######################################################################################################
prompt # Created by Francisco Riccio
prompt ######################################################################################################
prompt # Last Modifications: 
prompt ######################################################################################################
prompt # --> YCURI		20130829
prompt # --> JSANTILLAN	20151125
prompt ######################################################################################################
prompt # The installer will create a log ibm_3.0.0_$SID_$HOSTNAME_$DATE. Please Review and Report any error.
prompt ######################################################################################################
execute DBMS_LOCK.sleep(10)

declare
 v_sql varchar(4000):='';
 cursor c_sesiones is select 'alter system disconnect session '||q'[']'||sid||','||serial#||q'[']'||' immediate' as kill_sql from v$session where username='IBM';
begin
 for c in c_sesiones loop
  execute immediate(c.kill_sql);
 end loop;
end;
/

@?/rdbms/admin/catblock.sql
@?/rdbms/admin/utllockt.sql
@?/rdbms/admin/utlmail.sql
@?/rdbms/admin/prvtmail.plb


declare
 v_existe number:=0;
begin
 select count(1) as existe into v_existe from dba_users where username='IBM';
 if (v_existe>0) then
  --IBM_HISTORIAL_TBS 
  select count(1) as existe into v_existe from dba_tables where table_name = 'IBM_HISTORIAL_TBS' and owner = 'IBM';
  if (v_existe > 0) then
   select count(1) as existe into v_existe from dba_tables where table_name = 'IBM_AUX_HISTORIAL_TBS' and owner = 'SYS';
   if (v_existe > 0) then
    execute immediate 'drop table SYS.IBM_AUX_HISTORIAL_TBS';
   end if;
   execute immediate 'create table SYS.IBM_AUX_HISTORIAL_TBS as select * from IBM.IBM_HISTORIAL_TBS'; 
  end if;
  --IBM_HISTORIAL_TIEMPO_RESPUESTA
  select count(1) as existe into v_existe from dba_tables where table_name = 'IBM_HISTORIAL_TIEMPO_RESPUESTA' and owner = 'IBM';
  if (v_existe > 0) then
   select count(1) as existe into v_existe from dba_tables where table_name = 'IBM_AUX_HIST_TIEMPO_RESPUESTA' and owner = 'SYS';
   if (v_existe > 0) then
    execute immediate 'drop table SYS.IBM_AUX_HIST_TIEMPO_RESPUESTA';
   end if;
   execute immediate 'create table SYS.IBM_AUX_HIST_TIEMPO_RESPUESTA as select * from IBM.IBM_HISTORIAL_TIEMPO_RESPUESTA'; 
  end if;  
  --Eliminacion del usuario IBM  
  execute immediate 'drop user IBM cascade';
  --Eliminacion del profile PERFIL_IBM
  execute immediate 'drop profile PERFIL_IBM';  
  begin
   execute immediate 'drop profile IBM';  
     exception
   when others then
    null;
  end;
 end if;
end;
/

create profile PERFIL_IBM limit PASSWORD_GRACE_TIME unlimited;
alter profile PERFIL_IBM limit LOGICAL_READS_PER_SESSION unlimited; 
alter profile PERFIL_IBM limit SESSIONS_PER_USER 1; 
alter profile PERFIL_IBM limit PASSWORD_REUSE_MAX unlimited; 
alter profile PERFIL_IBM limit CONNECT_TIME unlimited; 
alter profile PERFIL_IBM limit CPU_PER_CALL unlimited; 
alter profile PERFIL_IBM limit PASSWORD_VERIFY_FUNCTION null;
alter profile PERFIL_IBM limit CPU_PER_SESSION unlimited; 
alter profile PERFIL_IBM limit COMPOSITE_LIMIT unlimited; 
alter profile PERFIL_IBM limit PASSWORD_LIFE_TIME unlimited; 
alter profile PERFIL_IBM limit PRIVATE_SGA unlimited; 
alter profile PERFIL_IBM limit PASSWORD_REUSE_TIME unlimited; 
alter profile PERFIL_IBM limit IDLE_TIME unlimited; 
alter profile PERFIL_IBM limit LOGICAL_READS_PER_CALL unlimited; 
alter profile PERFIL_IBM limit PASSWORD_LOCK_TIME unlimited; 
alter profile PERFIL_IBM limit FAILED_LOGIN_ATTEMPTS unlimited;

create user IBM identified by oracle10g default tablespace SYSAUX profile PERFIL_IBM;
grant connect, resource to IBM;
grant execute on utl_mail to IBM;
grant execute on utl_file to IBM;
grant execute on dbms_stats to IBM;
grant execute on dbms_system to IBM;
grant execute on dbms_lock to IBM;
grant create synonym to IBM;
grant create sequence to IBM;
grant create job to IBM;
grant select any dictionary to IBM;
grant create any directory to IBM;
grant analyze any dictionary to IBM;
grant alter system to IBM;
grant alter any procedure to IBM;
grant alter any table to IBM;
grant alter any index to IBM;
grant alter any trigger to IBM;
grant alter any materialized view to IBM;
grant alter any type to IBM;
grant alter any library to IBM;
grant analyze any to IBM;
grant analyze any dictionary to IBM;
grant unlimited tablespace to IBM;
grant debug any procedure to IBM;
grant alter database to IBM;
grant administer database trigger to IBM;
grant create trigger to IBM;
grant create external job to IBM;

declare
 v_version number;
begin
 select substr(VERSION,1,2) into v_version from product_component_version where product like '%Database%'; 
 if (v_version>=11) then
  execute immediate('grant execute on dbms_network_acl_admin to IBM');
 end if;
end;
/

connect IBM/oracle10g;

create table IBM_LOG (objeto varchar(255), error varchar(4000), fecha char(8));
create table IBM_HISTORIAL_TBS (nombre varchar(30) not null, tamano_free_mb number not null, tamano_ocupa_mb number not null, tamano_reservado_mb number not null, fecha char(8) not null);
create table IBM_OBJETOS_INV (nombre varchar(30) not null, owner varchar(30) not null, tipo varchar(30) not null, fecha_ddl char(8) not null, fecha char(8) not null);
create table IBM_TABLAS_FRAGMENTADAS(owner varchar(30) not null, nombre varchar(30) not null, tamano_mb number not null, porc_frag number not null, fecha char(8) not null);
create table IBM_INDICES_DESBALANCEADOS(owner varchar(30) not null, tabla varchar(30) not null, indice varchar(30) not null, particion varchar(30) not null, fecha char(8) not null);
create table IBM_BLOQUES_CORRUPTOS(file# number not null, block# number not null, blocks number not null, corruption_change# number not null, corruption_type varchar(9) not null, fecha char(8) not null);
create table IBM_INDICES_INVALIDOS_UNUSABLE (owner varchar(30) not null, tabla varchar(30) not null, indice varchar(30) not null, particion varchar(30) not null, estado varchar(30) not null, fecha char(8) not null);
create table IBM_TABLA_MIGRATION (owner varchar(30) not null, tabla varchar(30) not null, chain_cnt number not null, porc_chain_cnt number not null, fecha char(8) not null);
create table IBM_HISTORIAL_TIEMPO_RESPUESTA (fecha date not null, aas number not null);

commit;

create or replace package IBM is
 --Guarda eventos de error.
 procedure spu_guardar_log(pobjeto varchar, perror varchar); 
 --Crea la tabla TABLAS_FRAGMENTADAS el cual guarda las tablas candidatas a fragmentar.
 procedure spu_tablas_desfrag;
 --Guarda el tamaño de los tablespaces en la tabla HISTORIAL_TBS.
 procedure spu_historial_tbs;
 --Monitorea los objetos invalidos y si hay nuevos objetos respecto al dia anterior los recompila y los que no puede lo notifica. 
 procedure spu_monitoreo_obj_invalidos; 
 --Monitorea los indices que no esten en estado invalido o unusable.
 procedure spu_indices_inv_unusable;
 --Monitorea los indices desbalanceados.
 procedure spu_indices_desbalanceados;
 --Monitorea la existencia de bloques corruptos.
 procedure spu_bloques_corruptos;
 --Monitorea que tablas estan sufriendo de row chainning y migration.
 procedure spu_tabla_chain_migration;
 --Backup del control file.
 procedure spu_backup_controlfile;
 --Estadisticas de tablas.
 procedure spu_estadistica_tabla;
 --Captura del tiempo de respuesta.
 procedure spu_monitoreo_respuesta;
end;
/

create or replace package body IBM is

 function ibm_isEBS return boolean
 --Programado por Francisco Riccio. 
 is
  v_total number:=0;  
 begin
  select count(1) as total into v_total from dba_objects where owner = 'APPS' and object_name = 'FND_STATS';
  if (v_total=0) then
   return false;
  else
   return true;
  end if;
 end;

 function ibm_isPS return boolean
 --Programado por Francisco Riccio. 
 is
  v_total number:=0;  
 begin
  select count(1) as total into v_total from dba_objects where owner = 'SYSADM' and object_name = 'PSCBO_STATS';
  if (v_total=0) then
   return false;
  else
   return true;
  end if;
 end;

 procedure spu_guardar_log(pobjeto varchar, perror varchar) 
 is 
  pragma autonomous_transaction;
 --Programado por Francisco Riccio. 
 begin
  insert into IBM_LOG(objeto,error,fecha) values (pobjeto,perror,to_char(sysdate,'YYYYMMDD'));
  commit; 
 end;

 procedure spu_historial_tbs
 --Programado por Francisco Riccio.
 is
  type tbs is record
  (
   nombre dba_tablespaces.tablespace_name%type,
   tamano_free_mb number,
   tamano_ocupa_mb number,
   tamano_reservado_mb number,
   fecha date
  );
  v_tbs tbs; 
  cursor v_cursor is
  select nombre, round(tamano_libre_mb,2) as tamano_libre_mb, round(tamano_reservado_mb-tamano_libre_mb,2) as tamano_ocupado_mb, round(tamano_reservado_mb,2) as tamano_reservado_mb
  from
  (
   select nombre, tamano_libre_mb, 
   ( 
    select tamano_reservado_mb
    from
    (
     (select tablespace_name, sum(bytes)/1024/1024 as tamano_reservado_mb
     from dba_data_files
     group by tablespace_name)
    )
    where tablespace_name=nombre 
   ) as tamano_reservado_mb
   from (
    select tablespace_name nombre, sum(bytes)/1024/1024 as tamano_libre_mb
    from dba_free_space
    group by tablespace_name    
   )
  );
  v_existe_tabla number:=0;
 begin
  for c in v_cursor
  loop
   v_tbs.nombre:=c.nombre;
   v_tbs.tamano_free_mb:=c.tamano_libre_mb;
   v_tbs.tamano_ocupa_mb:=c.tamano_ocupado_mb;
   v_tbs.tamano_reservado_mb:=c.tamano_reservado_mb;   
   insert into IBM_HISTORIAL_TBS (nombre,tamano_free_mb,tamano_ocupa_mb,tamano_reservado_mb,fecha) values (v_tbs.nombre, v_tbs.tamano_free_mb, v_tbs.tamano_ocupa_mb, v_tbs.tamano_reservado_mb,to_char(sysdate,'YYYYMMDD'));
  end loop;
  commit;
 end;

 procedure spu_tablas_desfrag
 --Programado por Francisco Riccio.
 is
  cursor c_all_desfragmentacion is 
   select ds.owner, dt.table_name, 
    round(bytes/1024/1024,2) as tamano_mb,
    case 
     when nvl(ds.blocks,0)-nvl(dt.empty_blocks,0)=0 then 0    
    else
     round(((nvl(ds.blocks,0)-nvl(dt.empty_blocks,0))-nvl(dt.blocks,0))/(nvl(ds.blocks,0)-nvl(dt.empty_blocks,0)),2)  
    end as ratio
   from dba_tables dt join dba_segments ds on ds.segment_name = dt.table_name and ds.owner = dt.owner and ds.segment_type='TABLE'
   where round(bytes/1024/1024,2) >= 512
   order by tamano_mb desc, ratio desc, owner;    
 begin
  for c in c_all_desfragmentacion
  loop
   if (c.ratio>0.6) then
    insert into IBM_TABLAS_FRAGMENTADAS(owner,nombre,tamano_mb,porc_frag,fecha) values (c.owner,c.table_name,c.tamano_mb,to_char(c.ratio),to_char(sysdate,'YYYYMMDD'));
   end if;
  end loop;      
  commit;
 end;

 procedure spu_monitoreo_obj_invalidos
 --Programado por Francisco Riccio.
 is
  v_inicial number:=0;
  v_final number:=1;
  v_error1 exception;       
  v_error2 exception;       
  v_error3 exception;       
  v_error4 exception;
  v_error5 exception;
  v_encontro boolean:=false;
  pragma exception_init(v_error1,-24344);
  pragma exception_init(v_error2,-29501);
  pragma exception_init(v_error3,-01031);  
  pragma exception_init(v_error4,-04021);  
  pragma exception_init(v_error5,-00999);  
  v_texto varchar(32767):='';
 begin
  delete from IBM_OBJETOS_INV where fecha=to_char(sysdate,'YYYYMMDD');   
  commit;
  while (v_inicial<>v_final) loop
   select count(1) as total into v_inicial from dba_objects where status='INVALID' and object_type not in ('SYNONYM','INDEX');    
   for c in (select 'alter '||decode(tipo,'PACKAGE BODY','PACKAGE',tipo)||' '||owner||'.'||nombre||' '||decode(tipo,'PACKAGE BODY','compile body','compile') as sql_comp, 
    owner, nombre, tipo from(
     select owner, object_name as nombre, object_type as tipo from dba_objects where status='INVALID' and object_type not in ('SYNONYM','INDEX')
   minus
   select owner, nombre, tipo from IBM_OBJETOS_INV where fecha = to_char(sysdate-1,'YYYYMMDD'))
  )
   loop
    begin
     execute immediate (c.sql_comp);  
    exception
     when v_error1 or v_error2 or v_error3 or v_error4 or v_error5 then
      null;
    end;      
   end loop;    
   select count(1) as total into v_final from dba_objects where status='INVALID' and object_type not in ('SYNONYM','INDEX');    
  end loop;
  insert into IBM_OBJETOS_INV (nombre,owner,tipo,fecha_ddl,fecha)
   select object_name,owner,object_type,to_char(last_ddl_time,'YYYYMMDD') as fecha_ddl,to_char(sysdate,'YYYYMMDD') as fecha from dba_objects where status='INVALID' and object_type not in ('SYNONYM','INDEX');
  commit;
 end;

 procedure spu_indices_inv_unusable
 --Programado por Francisco Riccio.
 is
  v_texto varchar(4000):='';
  v_total number:=0;
 begin
  select count(1) as total into v_total from dba_indexes di where di.status in ('INVALID','UNUSABLE');
  if (v_total>0) then 
   for c in (select owner, table_name, index_name, status, to_char(sysdate,'YYYYMMDD') as fecha from dba_indexes di where di.status in ('INVALID','UNUSABLE')) 
   loop
    insert into IBM_INDICES_INVALIDOS_UNUSABLE(owner,tabla,indice,particion,estado,fecha) values (c.owner,c.table_name,c.index_name,'N/A',c.status,c.fecha);   
    commit;
   end loop;
  end if;
  select count(1) as total into v_total from dba_ind_partitions dip join dba_indexes di on dip.index_name = di.index_name and dip.index_owner = di.table_owner and dip.status in ('INVALID','UNUSABLE');
  if (v_total>0) then
   for c in (select dip.index_owner, di.table_name, dip.index_name, dip.partition_name, dip.status, to_char(sysdate,'YYYYMMDD') as fecha from dba_ind_partitions dip join dba_indexes di on dip.index_name = di.index_name and dip.index_owner = di.table_owner and dip.status in ('INVALID','UNUSABLE')) 
   loop
    insert into IBM_INDICES_INVALIDOS_UNUSABLE(owner,tabla,indice,particion,estado,fecha) values (c.index_owner,c.table_name,c.index_name,c.partition_name,c.status,c.fecha);   
   end loop;
   commit;   
  end if;  
  select count(1) as total into v_total from IBM_INDICES_INVALIDOS_UNUSABLE where fecha = to_char(sysdate,'YYYYMMDD');
  if (v_total>0) then
   for c in (select owner||'.'||tabla as tabla, indice, particion, estado from IBM_INDICES_INVALIDOS_UNUSABLE where fecha = to_char(sysdate,'YYYYMMDD'))
   loop 
    if (length(c.particion) > 0) then
     v_texto := 'TABLA = '||c.tabla||', INDICE = '||c.indice||', PARTICION = '||c.particion||', ESTADO = '||c.estado||chr(10)||chr(13);  
    else
     v_texto := 'TABLA = '||c.tabla||', INDICE = '||c.indice||', PARTICION = N/A, ESTADO = '||c.estado||chr(10)||chr(13);  
    end if;
   end loop; 
  end if;
 end;
  
 procedure spu_indices_desbalanceados
 --Programado por Francisco Riccio.
 is
  cursor c_indices is select owner,index_name,table_name from dba_indexes;
  cursor c_indices_partic is select di.table_name,dip.index_owner,dip.index_name,dip.partition_name from dba_ind_partitions dip join dba_indexes di on dip.index_name = di.index_name and dip.index_owner = di.table_owner;
  v_filas_borradas number;
  v_total_filas number;
  v_alt_arbol number;
  v_lista_indices varchar(4000):='';
  v_nombre_particion index_stats.partition_name%type;
 begin
  for c in c_indices loop
   begin
    execute immediate 'analyze index '||c.owner||'.'||c.index_name||' validate structure';
    select del_lf_rows, lf_rows, height, partition_name into v_filas_borradas, v_total_filas, v_alt_arbol, v_nombre_particion from index_stats;
    if (v_alt_arbol>3) or (v_filas_borradas/v_total_filas > 0.2) then
     insert into IBM_INDICES_DESBALANCEADOS(owner,tabla,indice,particion,fecha) values (c.owner,c.table_name,c.index_name,nvl(v_nombre_particion,''),to_char(sysdate,'YYYYMMDD'));
     v_lista_indices:=v_lista_indices||'OWNER = '||c.owner||', TABLA = '||c.table_name||', INDEX = '||c.index_name||chr(10)||chr(13);
    end if;
   end;
  end loop;
  for c in c_indices_partic loop
   begin
    execute immediate 'analyze index '||c.index_owner||'.'||c.index_name||' partition ('||c.partition_name||') validate structure';  
    select del_lf_rows, lf_rows, height, partition_name into v_filas_borradas, v_total_filas, v_alt_arbol, v_nombre_particion from index_stats;
    if (v_alt_arbol>3) or (v_filas_borradas/v_total_filas > 0.2) then
     insert into IBM_INDICES_DESBALANCEADOS(owner,tabla,indice,particion,fecha) values (c.index_owner,c.table_name,c.index_name,nvl(c.partition_name,''),to_char(sysdate,'YYYYMMDD'));
     v_lista_indices:=v_lista_indices||'OWNER = '||c.index_owner||', TABLA = '||c.table_name||', INDEX = '||c.index_name||', PARTICION = '||c.partition_name||chr(10)||chr(13);    
    end if;
   end;   
  end loop; 
  commit; 
 end;

 procedure spu_bloques_corruptos
 --Programado por Francisco Riccio.   
 is
  v_existe_bloques boolean:=false;
  cursor c_bloques_corruptos is select file#, block#, blocks, corruption_change#, corruption_type from v$database_block_corruption;  
 begin
  for c in c_bloques_corruptos loop
   insert into IBM_BLOQUES_CORRUPTOS(file#,block#,blocks,corruption_change#,corruption_type,fecha) values (c.file#,c.block#,c.blocks,c.corruption_change#,c.corruption_type,to_char(sysdate,'YYYYMMHH'));
   v_existe_bloques:=true;
  end loop; 
  if (v_existe_bloques=true) then
   commit;  
  end if;
  exception
   when others then 
    spu_guardar_log('spu_bloques_corruptos',SQLERRM);   
 end;
 
 procedure spu_tabla_chain_migration
 --Programado por Francisco Riccio. 
 is
  v_texto varchar(4000):=''; 
 begin
  insert into IBM_TABLA_MIGRATION(owner, tabla, chain_cnt, porc_chain_cnt, fecha)
   select owner, table_name, chain_cnt, porc_chain_cnt, fecha
   from
   (
    select owner, table_name, chain_cnt, round(chain_cnt/num_rows,2) as porc_chain_cnt, to_char (sysdate,'YYYYMMDD') as fecha
    from dba_tables
    where (round(chain_cnt/decode(num_rows,0,1,num_rows),2) > 0) and (owner not in ('SYS','SYSMAN'))
    minus
    select owner, tabla, chain_cnt, porc_chain_cnt, fecha
    from IBM_TABLA_MIGRATION
    where fecha in(
     select fecha from ( select fecha from IBM_TABLA_MIGRATION order by fecha desc ) where rownum <= 1)
   ) tm
   where exists (select owner, table_name from dba_tab_columns where owner=tm.owner and table_name=tm.table_name and data_type not in ('BLOB','CLOB','RAW','LONG RAW','LONG'));
   commit;
  for c in (select owner, tabla, chain_cnt, porc_chain_cnt, fecha from IBM_TABLA_MIGRATION where fecha = to_char(sysdate,'YYYYMMDD') order by porc_chain_cnt desc)
  loop
   v_texto:=v_texto||'TABLA = '||c.owner||'.'||c.tabla||', PORCENTAJE CHAIN_CNT = '||c.porc_chain_cnt||chr(10)||chr(13);
  end loop;
 end;

 procedure spu_backup_controlfile
 --Programado por Francisco Riccio.
 is
  v_ruta v$parameter.value%type; 
 begin
  select value into v_ruta from v$parameter where name='background_dump_dest';
  execute immediate ('alter database backup controlfile to '||q'[']'||v_ruta||'/control_bk.ctl'||q'[']'||' reuse');
 end;
 
 procedure spu_estadistica_tabla
 --Programado por Francisco Riccio.  
 is
  cursor c_estadistica_tabla is 
  select owner, table_name 
  from dba_tab_statistics 
  where ((sysdate - last_analyzed > 7) or (last_analyzed is null)) and (owner <> 'SYS');
  v_sql varchar(4000) := '';
 begin

  if (ibm_isPS()=true) then
	   execute immediate ('alter session set current_schema=SYSADM');
	   --for c in c_estadistica_tabla loop
	    begin
	     v_sql:='sysadm.pscbo_stats.gather_schema_stats()';
	     execute immediate('begin '||v_sql||'; end;');
	    exception
	     when others then
	      spu_guardar_log('spu_estadistica_tabla',SQLERRM);
	    end;
	   --end loop;
  else

	  if (ibm_isEBS()=true) then
	   execute immediate ('alter session set current_schema=APPS');
	   for c in c_estadistica_tabla loop
	    begin
	     v_sql:='apps.fnd_stats.gather_table_stats(ownname=>'||q'[']'||c.owner||q'[']'||',tabname=>'||q'['"]'||c.table_name||q'["']'||',percent=>dbms_stats.auto_sample_size,cascade=>true,degree=>2)';
	     execute immediate('begin '||v_sql||'; end;');
	    exception
	     when others then
	      if abs(SQLCODE)<>abs(25191) then
	       spu_guardar_log('spu_estadistica_tabla',SQLERRM);
	      end if;
	    end;
	   end loop;  
	  else
	   for c in c_estadistica_tabla loop
	   begin
	    dbms_stats.gather_table_stats(c.owner,'"'||c.table_name||'"',estimate_percent=>dbms_stats.auto_sample_size,cascade=>true,degree=>DBMS_STATS.AUTO_DEGREE);
	   exception
	    when others then
	     spu_guardar_log('spu_estadistica_tabla',SQLERRM);  
	    end;
	   end loop;
	  end if;
  end if;
  dbms_stats.gather_schema_stats('SYS',estimate_percent=>dbms_stats.auto_sample_size,degree=>2);
  dbms_stats.gather_dictionary_stats;  
  dbms_stats.gather_fixed_objects_stats;
 exception
  when others then
   spu_guardar_log('spu_estadistica_tabla',SQLERRM);       
 end;
 
 procedure spu_monitoreo_respuesta
 --Programado por Francisco Riccio.
 is
  v_aas number:=0;
  v_aas_min number:=0;
  v_aas_total number:=0;
  v_cpu number:=0;
  v_num number:=0;
  v_total number:=0;
  v_queries_top clob;
  cursor c_queries_top is
   select *
   from (
  select 'INST_ID='||to_char(ss.inst_id)||', SESSION='||to_char(ss.sid)||','||to_char(ss.serial#)||', PID='||to_char(spid)||', USUARIO='||ss.username||', PROGRAMA='||
  nvl(ss.program,'N/A')||', DBTIME='||to_char(dbtime)||', PCTLOAD='||to_char(pctload)||', WAIT='||ss.event||', SQL='||sql_text as info
  from ((gv$session ss join gv$sqlarea sa on sa.SQL_ID = ss.SQL_ID and ss.inst_id=sa.inst_id) join gv$process p on p.addr = ss.paddr and p.inst_id = ss.inst_id) join
  (
  select sql_id, count(*) DBTime, round(count(*)*100/sum(count(*)) over (), 2) pctload
  from gv$active_session_history
  where sample_time > sysdate - 1/24/60
  and session_type <> 'BACKGROUND'
  group by sql_id
  order by count(*) desc
  ) rs on rs.sql_id = sa.sql_id
  where ss.program not like '%PZ99%'
  order by dbtime desc, pctload desc
   )
   where rownum<11;
  cursor c_tiempo_actual(pinst_id number) is
   select round((((select value from gv$sys_time_model where stat_name = 'DB time' and inst_id=pinst_id)-dbtime)/1000000/60)/
          ((sysdate+(systimestamp-end_interval_time)-sysdate)*24*60),1) as average_active_session
   from
    (select a.value as dbtime, b.end_interval_time
     from DBA_HIST_SYS_TIME_MODEL a join DBA_HIST_SNAPSHOT b on a.snap_id = b.snap_id
     where (a.stat_name = 'DB time') and (a.instance_number=pinst_id) and (b.instance_number=pinst_id)
     order by b.end_interval_time desc
    )
   where rownum<2;  
 begin
  v_aas_total:=0;
  select value into v_cpu from v$parameter where name like 'cpu_count';
  for c in (select inst_id from gv$instance) loop
   open c_tiempo_actual(c.inst_id);
   fetch c_tiempo_actual into v_aas;
   v_aas_total:=v_aas+v_aas_total;
   close c_tiempo_actual;
  end loop;
  select count(1) as total into v_total from gv$instance;  
  v_aas:=round(v_aas_total/v_total,2);
  if (v_aas<0) then
   v_aas:=0;
  end if;
  insert into IBM_HISTORIAL_TIEMPO_RESPUESTA(fecha,aas) values (sysdate,v_aas);
  commit;  
  if (v_aas>v_cpu) then
   begin  
    v_num:=0;
    for c in c_queries_top loop
     v_num:=v_num+1;
     if (v_num=1) then
      v_queries_top:=v_queries_top||'TOP SESSIONS:'||chr(10)||chr(13)||chr(10)||chr(13);
     end if;
     v_queries_top:=v_queries_top||c.info||chr(10)||chr(13)||chr(10)||chr(13);
    end loop;
    commit;  
   end;  
   for c in (select inst_id from gv$instance) loop  
    select aas as average_active_session
    into v_aas_min
    from
    (
     select round((dbtime - (lead(dbtime,1,0) over (order by snap_id desc)))/minutos,1) as aas
     from (
      select a.snap_id, a.value/1000000/60 as dbtime, (sysdate+(end_interval_time-begin_interval_time)-sysdate)*24*60 as minutos
      from DBA_HIST_SYS_TIME_MODEL a join DBA_HIST_SNAPSHOT b on a.snap_id = b.snap_id and a.instance_number=c.inst_id
      where a.stat_name = 'DB time') d 
    )
    where rownum<2;         
   end loop;
  end if;    
 end;
 
end IBM;
/

show errors;

connect / as sysdba

create or replace procedure sys.spu_estadistica_sis
--Programado por Francisco Riccio. 
is
 v_sql varchar(4000):='';
begin
 begin
  v_sql:=q'[begin dbms_stats.gather_system_stats('interval', interval=>5); end;]';
  execute immediate v_sql;
  v_sql:='begin dbms_stats.gather_system_stats(); end;';
  execute immediate v_sql;
 exception
  when others then
   ibm.ibm.spu_guardar_log('spu_estadistica_sis',SQLERRM);         
 end;  
end;
/

grant execute on sys.spu_estadistica_sis to IBM;

connect IBM/oracle10g

create synonym spu_estadistica_sis for sys.spu_estadistica_sis;

execute dbms_scheduler.create_schedule(schedule_name=>'IBM_sched_20_min',start_date=>trunc(SYSTIMESTAMP,'day'),repeat_interval=>'freq=minutely;bysecond=00;interval=20',end_date=>null,comments=>'Frequencia 20 minutos');
execute dbms_scheduler.create_schedule(schedule_name=>'ibm_sched_30_min',start_date=>trunc(SYSTIMESTAMP,'day'),repeat_interval=>'freq=minutely;bysecond=00;interval=30',end_date=>null,comments=>'Frequencia 30 minutos');
execute dbms_scheduler.create_schedule(schedule_name=>'IBM_sched_12_hor',start_date=>trunc(SYSTIMESTAMP,'day'),repeat_interval=>'freq=hourly;byminute=00;bysecond=00;interval=12',end_date=>null,comments=>'Frequencia cada 12 horas');
execute dbms_scheduler.create_schedule(schedule_name=>'IBM_sched_01_dia',start_date=>trunc(SYSTIMESTAMP,'day'),repeat_interval=>'freq=daily;byhour=00;byminute=00;bysecond=00',end_date=>null,comments=>'Frequencia diaria');
execute dbms_scheduler.create_schedule(schedule_name=>'IBM_sched_semanal',start_date=>trunc(SYSTIMESTAMP,'day'),repeat_interval=>'freq=weekly;byday=sun;byhour=00;byminute=00;bysecond=00',end_date=>null,comments=>'Frequencia semanal');
execute dbms_scheduler.create_schedule(schedule_name=>'IBM_sched_1ro_men',start_date=>trunc(SYSTIMESTAMP,'day'),repeat_interval=>'freq=monthly;bymonthday=1;byhour=00;byminute=00;bysecond=00',end_date=>null,comments=>'Frequencia el 1er dia del mes');
execute dbms_scheduler.create_schedule(schedule_name=>'IBM_sched_03_men',start_date=>trunc(SYSTIMESTAMP,'day'),repeat_interval=>'freq=monthly;bymonthday=1;byhour=00;byminute=00;bysecond=00;interval=3',end_date=>null,comments=>'Frequencia primer dia de cada 3 meses');

commit;

execute dbms_scheduler.create_program(program_name=>'IBM_prog_monitoreo_desfrag',program_type=>'stored_procedure',program_action=>'IBM.spu_tablas_desfrag',enabled=>true,comments=>'Monitoreo de Desfragmentacion de Tablas');
execute dbms_scheduler.create_program(program_name=>'IBM_prog_historial_tbs',program_type=>'stored_procedure',program_action=>'IBM.spu_historial_tbs',enabled=>true,comments=>'Registro del tamano de los tablespaces');
execute dbms_scheduler.create_program(program_name=>'IBM_prog_monitoreo_obj_inv',program_type=>'stored_procedure',program_action=>'IBM.spu_monitoreo_obj_invalidos',enabled=>true,comments=>'Monitoreo de Objetos Invalidos');
execute dbms_scheduler.create_program(program_name=>'IBM_prog_ind_inv_un',program_type=>'stored_procedure',program_action=>'IBM.spu_indices_inv_unusable',enabled=>true,comments=>'Monitoreo de indices en estado INVALID o UNUSABLE');
execute dbms_scheduler.create_program(program_name=>'IBM_prog_ind_desbalanceados',program_type=>'stored_procedure',program_action=>'IBM.spu_indices_desbalanceados',enabled=>true,comments=>'Monitoreo de indices desbalanceados');
execute dbms_scheduler.create_program(program_name=>'ibm_prog_bloques_corruptos',program_type=>'stored_procedure',program_action=>'ibm.spu_bloques_corruptos',enabled=>true,comments=>'Monitoreo de Bloques Corruptos');
execute dbms_scheduler.create_program(program_name=>'IBM_prog_chain_migration',program_type=>'stored_procedure',program_action=>'IBM.spu_tabla_chain_migration',enabled=>true,comments=>'Monitoreo de Row Chainning y Migration');
execute dbms_scheduler.create_program(program_name=>'ibm_prog_backup_controlfile',program_type=>'stored_procedure',program_action=>'ibm.ibm.spu_backup_controlfile',enabled=>true,comments=>'Backup del controlfile.');
execute dbms_scheduler.create_program(program_name=>'ibm_prog_estadistica_tabla',program_type=>'stored_procedure',program_action=>'ibm.spu_estadistica_tabla',enabled=>true,comments=>'Estadisticas de tablas.');
execute dbms_scheduler.create_program(program_name=>'IBM_prog_estadistica_sis',program_type=>'stored_procedure',program_action=>'spu_estadistica_sis',enabled=>true,comments=>'Monitoreo de estadisticas de Sistema.');
execute dbms_scheduler.create_program(program_name=>'IBM_prog_monitoreo_respuesta',program_type=>'stored_procedure',program_action=>'IBM.spu_monitoreo_respuesta',enabled=>true,comments=>'Monitoreo del Tiempo de Respuesta.');

commit;

execute dbms_scheduler.create_job(job_name=>'IBM_job_monitoreo_desfrag',program_name=>'IBM_prog_monitoreo_desfrag',schedule_name=>'IBM_sched_semanal',enabled=>true,auto_drop=>false,comments=>'Monitoreo de Desfragmentacion de Tablas cada semana');  
execute dbms_scheduler.create_job(job_name=>'IBM_job_historial_tbs',program_name=>'IBM_prog_historial_tbs',schedule_name=>'IBM_sched_01_dia',enabled=>true,auto_drop=>false,comments=>'Registro del tamano de los tablespaces cada dia');  
execute dbms_scheduler.create_job(job_name=>'IBM_job_monitoreo_obj_inv',program_name=>'IBM_prog_monitoreo_obj_inv',schedule_name=>'IBM_sched_12_hor',enabled=>true,auto_drop=>false,comments=>'Monitoreo de Objetos Invalidos cada 12 horas');  
execute dbms_scheduler.create_job(job_name=>'IBM_job_monitoreo_ind_inv_un',program_name=>'IBM_prog_ind_inv_un',schedule_name=>'IBM_sched_01_dia',enabled=>true,auto_drop=>false,comments=>'Monitoreo de indices en estado INVALID o UNUSABLE cada dia');  
execute dbms_scheduler.create_job(job_name=>'IBM_job_ind_desbalanceados',program_name=>'IBM_prog_ind_desbalanceados',schedule_name=>'IBM_sched_1ro_men',enabled=>true,auto_drop=>false,comments=>'Monitoreo de indices desbalanceados el primer dia de cada mes');
execute dbms_scheduler.create_job(job_name=>'ibm_job_bloques_corruptos',program_name=>'ibm_prog_bloques_corruptos',schedule_name=>'ibm_sched_01_dia',enabled=>true,auto_drop=>false,comments=>'Monitoreo de Bloques Corruptos cada dia');  
execute dbms_scheduler.create_job(job_name=>'IBM_job_chain_migration',program_name=>'IBM_prog_chain_migration',schedule_name=>'IBM_sched_1ro_men',enabled=>true,auto_drop=>false,comments=>'Monitoreo de Row Chainning y Migration cada mes');
execute dbms_scheduler.create_job(job_name=>'IBM_job_depura_log',job_type=>'executable',job_action=>'/home/oracle/scripts/depuraTRC.sh',schedule_name=>'IBM_sched_1ro_men',enabled=>true,auto_drop=>false,comments=>'Depuracion de archivo log y trace');
execute dbms_scheduler.create_job(job_name=>'ibm_job_backup_controlfile',program_name=>'ibm_prog_backup_controlfile',schedule_name=>'ibm_sched_30_min',enabled=>true,auto_drop=>false,comments=>'Backup del controlfile cada 30 minutos'); 
execute dbms_scheduler.create_job(job_name=>'ibm_job_estadistica_tabla',program_name=>'ibm_prog_estadistica_tabla',schedule_name=>'ibm_sched_01_dia',enabled=>true,auto_drop=>false,comments=>'Monitoreo de estadisticas de tablas cada dia');  
execute dbms_scheduler.create_job(job_name=>'IBM_job_estadistica_sis',program_name=>'IBM_prog_estadistica_sis',schedule_name=>'IBM_sched_03_men',enabled=>true,auto_drop=>false,comments=>'Monitoreo de estadisticas de Sistema cada 3 meses');  
execute dbms_scheduler.create_job(job_name=>'IBM_job_monitoreo_respuesta',program_name=>'IBM_prog_monitoreo_respuesta',schedule_name=>'IBM_sched_20_min',enabled=>true,auto_drop=>false,comments=>'Monitoreo del Tiempo de Respuesta por hora');  

commit;

connect / as sysdba

declare
 v_existe number:=0;
 cursor c_schedule is select job_name from user_scheduler_jobs where job_name like '%IBM%';
 cursor c_trigger is select trigger_name from user_triggers where trigger_name like '%IBM%'; 
begin
 for c in c_schedule loop
  begin
   dbms_scheduler.drop_job(job_name=>c.job_name);  
  exception
   when others then
    null;
  end;
 end loop;
 for c in c_trigger loop
  begin 
   execute immediate 'drop trigger SYS.'||c.trigger_name;  
  exception
   when others then
    null;
  end;
 end loop; 
 begin
  select count(1) as existe into v_existe from dba_tables where table_name = 'IBM_AUX_HISTORIAL_TBS' and owner='SYS';
  if (v_existe > 0) then
   execute immediate 'insert into IBM.IBM_HISTORIAL_TBS select * from SYS.IBM_AUX_HISTORIAL_TBS';   
   execute immediate 'commit'; 
   execute immediate 'drop table SYS.IBM_AUX_HISTORIAL_TBS';
  end if;
 exception 
  when others then 
   null;
 end;
 begin
  select count(1) as existe into v_existe from dba_tables where table_name = 'IBM_AUX_HIST_TIEMPO_RESPUESTA' and owner='SYS';
  if (v_existe > 0) then
   execute immediate 'insert into IBM.IBM_HISTORIAL_TIEMPO_RESPUESTA select * from SYS.IBM_AUX_HIST_TIEMPO_RESPUESTA';
   execute immediate 'commit'; 
   execute immediate 'drop table SYS.IBM_AUX_HIST_TIEMPO_RESPUESTA';
  end if;  
 exception
  when others then
   null;
 end;
end;
/

prompt ######################################################################################
prompt # End of the IBM Notifier Installer. 
prompt ######################################################################################
prompt #Please Review the log IBM_3.0.0_$SID_$HOSTNAME_$DATE and report any error.
prompt ######################################################################################
execute DBMS_LOCK.sleep(5)
spool off
exit















-----informacion adicional

--Fragmentacion de Indices, solo si no tiene segment advisor configurado
CREATE OR REPLACE FUNCTION fu_tam_esperado_indx (
   vtablename     VARCHAR2,
   vtableowner    VARCHAR2,
   vindexname     VARCHAR2,
   vindexowner    VARCHAR2)
   RETURN NUMBER
IS
   v_index_leaf_estimate   NUMBER;
   vtargetuse     CONSTANT POSITIVE := 90;               -- igual a pctfree 10
   voverhead               NUMBER := 192;
   vblocksize              NUMBER;
BEGIN
   SELECT VALUE
     INTO vblocksize
     FROM v$parameter
    WHERE NAME = 'db_block_size';

     SELECT ROUND (
               100 / vtargetuse
               * (ind.num_rows * (tab.rowid_length + ind.uniq_ind + 4)
                  + SUM ( (tc.avg_col_len) * (tab.num_rows)) -- column data bytes
                                                            )
               / (vblocksize - 192)
            * 8192
            / 1024
            / 1024,2)
               index_leaf_estimate
       INTO v_index_leaf_estimate
       FROM (SELECT                                            /*+ no_merge */
                   table_name,
                    num_rows,
                    DECODE (partitioned, 'YES', 10, 6) rowid_length
               FROM dba_tables
              WHERE table_name = vtablename AND owner = vtableowner) tab,
            (SELECT                                            /*+ no_merge */
                   index_name,
                    index_type,
                    num_rows,
                    DECODE (uniqueness, 'UNIQUE', 0, 1) uniq_ind
               FROM dba_indexes
              WHERE     table_owner = vtableowner
                    AND table_name = vtablename
                    AND owner = vindexowner
                    AND index_name = vindexname) ind,
            (SELECT                                            /*+ no_merge */
                   column_name
               FROM dba_ind_columns
              WHERE     table_owner = vtableowner
                    AND table_name = vtablename
                    AND index_owner = vindexowner
                    AND index_name = vindexname) ic,
            (SELECT                                            /*+ no_merge */
                   column_name, avg_col_len
               FROM dba_tab_cols
              WHERE owner = vtableowner AND table_name = vtablename) tc
      WHERE tc.column_name = ic.column_name
   GROUP BY ind.num_rows, ind.uniq_ind, tab.rowid_length;


   RETURN v_index_leaf_estimate;
END fu_tam_esperado_indx;
/


