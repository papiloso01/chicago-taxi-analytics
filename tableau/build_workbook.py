"""Generate an editable Tableau TWB template; Desktop render validation is required."""
from pathlib import Path
import xml.etree.ElementTree as E

FIELDS={'trip_id':('string','dimension'),'trip_date':('date','dimension'),'trip_hour':('integer','dimension'),'taxi_id':('string','dimension'),'payment_type':('string','dimension'),'company':('string','dimension'),'pickup_community_area':('string','dimension'),'dropoff_community_area':('string','dimension'),'distance_km':('real','measure'),'trip_total':('real','measure'),'tips':('real','measure'),'trip_seconds':('real','measure')}
CALCS={'Average trip total':('real','measure','SUM([trip_total]) / COUNT([trip_id])'),
       'Longest trip km':('real','measure','MAX([distance_km])'),
       'Shortest positive trip km':('real','measure','MIN(IF [distance_km] > 0 THEN [distance_km] END)'),
       'Is longest trip':('boolean','dimension','[distance_km] = { FIXED : MAX([distance_km]) }'),
       'Is shortest positive trip':('boolean','dimension','[distance_km] > 0 AND [distance_km] = { FIXED : MIN(IF [distance_km] > 0 THEN [distance_km] END) }')}
DS='textscan.taxi'

def sub(parent,tag,**attrs):return E.SubElement(parent,tag,{k.replace('_','-'):str(v) for k,v in attrs.items()})
def col(parent,name):
    dtype,role=FIELDS.get(name,CALCS.get(name,('real','measure')))[:2]
    c=sub(parent,'column',datatype=dtype,name=f'[{name}]',role=role,type='nominal' if role=='dimension' else 'quantitative')
    if name in CALCS:sub(c,'calculation',**{'class':'tableau','formula':CALCS[name][2]})
    return c

def build():
    root=E.Element('workbook',{'source-build':'9.3.1','source-platform':'win','version':'9.3'})
    prefs=sub(root,'preferences');sub(prefs,'preference',name='ui.shelf.height',value=26)
    ds=sub(sub(root,'datasources'),'datasource',caption='Chicago Taxi Trips (SYNTHETIC SAMPLE)',inline='true',name=DS,version='9.3')
    cn=sub(ds,'connection',**{'class':'textscan','directory':'sample','filename':'trips.csv','separator':',','header':'yes','locale':'en_US','charset':'UTF-8'})
    sub(cn,'relation',name='trips.csv',table='[trips.csv]',type='table')
    for field in [*FIELDS,*CALCS]:col(ds,field)
    sheets=sub(root,'worksheets');windows=sub(root,'windows',source_height=30)
    specs=[('Trip count',None,'trip_id','Count','Text',None),('Reported totals USD',None,'trip_total','Sum','Text',None),('Average trip total USD',None,'Average trip total','User','Text',None),('Daily reported totals','trip_date','trip_total','Sum','Line',None),('Taxi rankings by reported total','taxi_id','trip_total','Sum','Bar',None),('Trips by payment method','payment_type','trip_id','Count','Bar',None),('Demand by hour','trip_hour','trip_id','Count','Bar',None),('Longest trip km',None,'Longest trip km','User','Text',None),('Shortest positive trip km',None,'Shortest positive trip km','User','Text',None),('Longest trip IDs','trip_id','distance_km','Max','Bar','Is longest trip'),('Shortest positive trip IDs','trip_id','distance_km','Min','Bar','Is shortest positive trip')]
    for name,dim,measure,agg,mark,filtername in specs:
        sheet=sub(sheets,'worksheet',name=name);table=sub(sheet,'table');view=sub(table,'view')
        sub(sub(view,'datasources'),'datasource',caption='Chicago Taxi Trips (SYNTHETIC SAMPLE)',name=DS)
        dep=sub(view,'datasource-dependencies',datasource=DS)
        used=[measure]+([dim] if dim else [])+([filtername] if filtername else [])
        aliases={}
        for field in used:
            col(dep,field);derivation=agg if field==measure else 'None'
            typ='qk' if field==measure else 'nk'
            token={'Count':'cnt','User':'usr'}.get(derivation,derivation.lower())
            alias=f'[{token}:{field}:{typ}]';aliases[field]=alias
            sub(dep,'column-instance',column=f'[{field}]',derivation=derivation,name=alias,pivot='key',type='quantitative' if field==measure else 'nominal')
        if filtername:
            fl=sub(view,'filter',**{'class':'categorical','column':f'[{DS}].{aliases[filtername]}'})
            sub(fl,'groupfilter',function='member',level=aliases[filtername],member='true')
        if name.startswith('Taxi rankings'):
            sub(view,'sort',**{'class':'computed','column':f'[{DS}].{aliases[dim]}','direction':'DESC','using':f'[{DS}].{aliases[measure]}'})
        sub(view,'aggregation',value='true');sub(table,'style')
        pane=sub(sub(table,'panes'),'pane');sub(sub(pane,'view'),'breakdown',value='auto');sub(pane,'mark',**{'class':mark})
        if mark=='Text':sub(sub(pane,'encodings'),'text',column=f'[{DS}].{aliases[measure]}')
        rows=sub(table,'rows');cols=sub(table,'cols')
        if mark!='Text':
            if mark=='Line':cols.text=f'[{DS}].{aliases[dim]}';rows.text=f'[{DS}].{aliases[measure]}'
            else:rows.text=f'[{DS}].{aliases[dim]}';cols.text=f'[{DS}].{aliases[measure]}'
        sub(windows,'window',**{'class':'worksheet','name':name})
    dash=sub(sub(root,'dashboards'),'dashboard',name='Chicago Taxi Overview — SYNTHETIC SAMPLE')
    sub(dash,'style');sub(dash,'size',maxheight=1500,maxwidth=1400,minheight=1500,minwidth=1400)
    zones=sub(dash,'zones')
    layout=[(0,0,33333,12000),(33333,0,33333,12000),(66666,0,33334,12000),(0,12000,65000,20000),(65000,12000,35000,20000),(0,32000,50000,18000),(50000,32000,50000,18000),(0,50000,50000,10000),(50000,50000,50000,10000),(0,60000,50000,40000),(50000,60000,50000,40000)]
    for i,(spec,box) in enumerate(zip(specs,layout),1):
        x,y,w,h=box;sub(zones,'zone',id=i,name=spec[0],x=x,y=y,w=w,h=h)
    sub(windows,'window',**{'class':'dashboard','name':'Chicago Taxi Overview — SYNTHETIC SAMPLE'})
    E.indent(root);out=Path(__file__).parent/'chicago-taxi.twb';E.ElementTree(root).write(out,encoding='utf-8',xml_declaration=True)
    print(out)
if __name__=='__main__':build()
