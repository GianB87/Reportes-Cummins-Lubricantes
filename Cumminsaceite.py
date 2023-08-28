import requests
# requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS = 'DES-CBC3-SHA'
import json
import pandas as pd
import numpy as np
from datetime import datetime as dt, timedelta
from tqdm import tqdm
import time


class Connection:
    """Clase que realiza la conexion a la api. """

    def __init__(self, username, password):
        """
        Al instanciar la clase debe indicar el usuario y la contraseña.

        :param username: nombre de usuario
        :type username: str
        :param password: contrasena
        :type password: str
        """

        self.username=username
        self.password=password
        self.token=''
        self.cookies=''
        self.api_auth="https://analyticsapi.cummins.cl/oauth/token"
        self.api_base="https://analyticsapi.cummins.cl/api/"

    def setToken(self, token):
        """
        Asigna el token que se utilizara para validar la conexion.
        :param token El token de acceso.
        """
        self.token=token

    def setCookies(self, cookies):
        """
        Asigna las cookies que se utilizara para validar la conexion.
        :param cookies las cookies de acceso.
        """
        self.cookies = cookies

    def getToken(self):
        """
        Generacion del Token necesario para la conexion.
        """
        username=self.username
        password=self.password
        payload='username='+username+'&password='+password+'&grant_type=password'
        headers = { 'Authorization': 'Basic YW5ndWxhcmFwcDoxMjM0NQ==',  'Content-Type': 'application/x-www-form-urlencoded','Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07' }

        response = requests.request("POST", self.api_auth, headers=headers, data=payload)
        if response.status_code == 200:
            token = response.json()['access_token']
            self.setToken(token)
        else:
            raise NameError('Error de autenticacion. Verifique sus credenciales.')



    def getInformeDiarioFaena(self, faena):
        """
        Obtencion del informe base segun la faena.

        :param faena: faena de la cual se quiere descargar el informe diario.
        :return: DataFrame informe diario de los ultimos 7 dias.
        :rtype: Dataframe
        """
        today=dt.today()
        if faena is 'Caserones':
            fechainicial=today-timedelta(days=14)
        else:
            fechainicial=today-timedelta(days=7)
        fechas=[]
        while fechainicial<today:
            fechas.append(fechainicial.strftime('%Y-%m-%d'))
            fechainicial=fechainicial+timedelta(days=1)
        df=pd.DataFrame()
        bar = tqdm(total = 7, desc=faena)
        for fecha in fechas:
            dfTemp=self.getInformeDiarioFaenaFecha(faena,fecha)
            df=pd.concat([df,dfTemp], ignore_index=True)
            time.sleep(3)  #no modificar
            bar.update(1)
        bar.close()
        return df
    
    def getInformeDiarioFaenaFecha(self, faena, fecha):
        """
        Obtiene el informe diario para un dia y faena en particular.

        :param faena:  faena a obtener.
        :type faena: String
        :param fecha: fecha a obtener. formato yyyy-mm-ddd
        :type fecha: String
        :return: Dataframe informe diario dia.
        :rtype: Dataframe
        """
        periodoInicial=[' 00:00:00',  ' 02:00:00', ' 04:00:00',  ' 06:00:00', ' 08:00:00', ' 10:00:00',  ' 12:00:00',  ' 14:00:00',  ' 16:00:00',  ' 18:00:00', ' 20:00:00', ' 22:00:00']
        periodoFinal=[' 02:00:00',  ' 04:00:00',  ' 06:00:00', ' 08:00:00', ' 10:00:00',  ' 12:00:00', ' 14:00:00', ' 16:00:00', ' 18:00:00', ' 20:00:00', ' 22:00:00', ' 23:59:59']
        df=pd.DataFrame()
        for periodo in periodoInicial:
            pos=periodoInicial.index(periodo)
            # print(periodo, pos)
            url = self.api_base+"backupparametros/informediario/faenas/"+faena+'/fechas/'+fecha+periodo+'/'+fecha+periodoFinal[pos]
            payload={}
            headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
            response = requests.request("GET", url, headers=headers, data=payload)
            # print(url)
            if response.status_code == 200:
                dfTemp=pd.read_json(response.text, dtype={'flota':'str','flota':str, 'unidad':'str', 'unidad':str})
                if len(dfTemp) > 0:
                    df=pd.concat([df,dfTemp], ignore_index=True)
                    del dfTemp
            else:
                raise NameError('Error en la descarga de datos. '+ str(response.status_code))

        return df

    def getMuestraById(self, idMuestra):
        """
        Obtiene la muestra por id.

        :param idMuestra: identificacion de la muestra.
        :type idMuestra: numeric
        :return: muestras con ese id.
        :rtype: Dataframe
        """
        url = self.api_base+"backupparametros/muestra/"+idMuestra
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getMuestraByIdFecha(self, idMuestra, fecha):
        """
        Devuelve las muestras con fecha mayor a la indicada.
        :param idMuestra: numero de identificacion de la muestra.
        :type idMuestra: numeric.
        :param fecha: fecha desde la cual buscar.
        :type fecha: string
        :return: muestras mayores a la fecha
        :rtype: Dataframe
        """
        url = self.api_base+"backupparametros/muestra/"+idMuestra
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text)
            return df
        else:
            raise NameError('Error en la descarga de datos.')   

    def getScaa(self):
        """
        Obtener la tabla scaa completa. No recomendable si no se tiene una excelente conexion a internet.

        """
        url = self.api_base+"scaa"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str, 'equipo':'str', 'equipo':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')  
  
    def getScaaFechaMaxima(self):
        '''Obtener la fecha maxima segun flota en scaa para cada faena.'''
        url = self.api_base+"scaa/fechamax/"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:     
            df=pd.read_json(response.text)  
            return df
        else:
            raise NameError('Error en la descarga de datos.')    
    
    def getScaaFaena(self, faena):
        '''Obtener la fecha maxima segun flota en scaa para cada faena.'''
        url = self.api_base+"scaa/faenas/"+faena
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:     
            df=pd.read_json(response.text,dtype={'flota':'str', 'flota':str, 'equipo':'str', 'equipo':str, 'esn':'str', 'esn':str})
            df.loc[(df['faena']=='Escondida')&(df['flota']=='930 E'), 'diluc']=0
            df.loc[(df['faena']=='Escondida')&(df['flota']=='960 E'), 'diluc']=0
            df.loc[(df['faena']=='Escondida')&(df['flota']=='980 E'), 'diluc']=0
            df.loc[(df['faena']=='Escondida')&(df['flota']=='930 E'), 'ptoinfl']=0
            df.loc[(df['faena']=='Escondida')&(df['flota']=='960 E'), 'ptoinfl']=0
            df.loc[(df['faena']=='Escondida')&(df['flota']=='980 E'), 'ptoinfl']=0
            
            return df
        else:
            raise NameError('Error en la descarga de datos.')        

    def getScaaFaenaFecha(self, faena, fecha):
        """
        Obtener la tabla scaa por faena desde la fecha indicada.

        :param faena: nombre de la faena a obtener.
        :param fecha: fecha desde la cual se descargara la data.
        :return: datos de aceite de la faena desde fecha indicada.
        :rtype: DataFrame
        """
        url = self.api_base+"scaa/faenas/"+faena+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'equipo':'str', 'flota':str, 'equipo':str})
            df.loc[(df['faena']=='Escondida')&(df['flota']=='930 E'), 'diluc']=0
            df.loc[(df['faena']=='Escondida')&(df['flota']=='960 E'), 'diluc']=0
            df.loc[(df['faena']=='Escondida')&(df['flota']=='930 E'), 'ptoinfl']=0
            df.loc[(df['faena']=='Escondida')&(df['flota']=='960 E'), 'ptoinfl']=0
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getScaaFaenaFechaEquipo(self, faena, equipo, fecha):
        """
        Obtener la tabla scaa por faena desde la fecha indicada, filtrando por un equipo en particular.

        :param faena: nombre de la faena a obtener.
        :type faena: string
        :param fecha: fecha desde la cual se descargara la data.
        :type fecha: string
        :param equipo: equipo del que se entregaran los resultados
        :type equipo: string
        :return: datos de aceite del equipo en la faena desde fecha indicada.
        :rtype: DataFrame
        """
        df=self.getScaaFaenaFecha(faena, fecha)
        df=df.loc[df['equipo']==equipo]
        return df


    def getScaaFaenaFechaEquipoParametros(self, faena, equipo, fechainicio, parametros):
        """
        Obtener la tabla scaa por faena desde la fecha indicada, filtrando por un equipo en particular.

        :param faena: nombre de la faena a obtener.
        :type faena: string
        :param fechainicio: fecha desde la cual se descargara la data.
        :type fechainicio: string
        :param equipo: equipo del que se entregaran los resultados
        :type equipo: string
        :param parametros: lista de parametros se entregaran como resultados.
        :type parametros: list
        :return: datos de aceite del equipo en la faena desde fecha indicada.
        :rtype: DataFrame
        """
        df=self.getScaaFaenaFechaEquipo(faena, equipo, fechainicio)
        listaParametros=['faena', 'equipo', 'fechamuestra']
        for parametro in parametros:
            listaParametros.append(parametro)
        #print(listaParametros)
        df=df[listaParametros]
        return df



    def getDbm(self):
        '''Obtener la fecha maxima segun flota en scaa para cada faena.'''
        url = self.api_base+"dbm"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str, 'unidad':'str', 'unidad':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')   
    

    def getDbmFaena(self, faena):
        '''Obtener la fecha maxima segun flota en scaa para cada faena.'''
        url = self.api_base+"dbm/faenas/"+faena
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str, 'unidad':'str', 'unidad':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')   

    def getLimites(self):
        """
        Retorna los valores de la tabla Limites.
        :return: Dataframe con los valores de la tabla limites.
        :rtype: dataframe
        """
        url = self.api_base+"limites"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')   

    def getLimitesRanking(self):
        """
        Retorna los valores de la tabla Limites Ranking.
        :return: Dataframe con los valores de limites ranking
        :rtype: dataframe
        """
        url = self.api_base+"limitesranking"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text,  dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.') 
    
    def getNominaEquipos(self):
        """
        Retorna los valores de la tabla nomina_equipos
        :return: Dataframe con los valores nomina equipos
        :rtype: dataframe
        """
        url = self.api_base+"nominaequipos"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')


    def getOneTeamScaa(self):
        '''Obtener la tabla oneteamscaa.'''
        url = self.api_base+"oneteamscaa"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.') 

    def getOneTeamSpecto(self):
        '''Obtener la tabla oneteamspecto.'''
        url = self.api_base+"oneteamspecto"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')        

    def getParametros(self):
        '''Obtener la tabla parametros.'''
        url = self.api_base+"parametro"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')  

    def getScaaLimites(self):
        '''Obtener la tabla scaalimites.'''
        url = self.api_base+"scaalimites"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')        

    def getScaaSinAnalisis(self):
        '''Obtener la tabla scaasinanalisis.'''
        url = self.api_base+"scaasinanalisis"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')         

    def getMontaje(self):
        '''Obtener la tabla montaje.'''
        url = self.api_base+"montajes"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.') 

    def getMiningDashboard(self):
        '''Obtener la tabla mining_dashboard.'''
        url = self.api_base+"miningdashboard"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.') 

    def getLoadfactor(self):
        '''Obtener la tabla loadfactor.'''
        url = self.api_base+"loadfactor"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getKpi(self):
        '''Obtener la tabla kpi.'''
        url = self.api_base+"kpi"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getInstalacionFit(self):
        '''Obtener la tabla instalacionfit.'''
        url = self.api_base+"instalacionfit"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getHorasMuestrasAceite(self):
        '''Obtener la tabla horas_muestras_aceite.'''
        url = self.api_base+"horasmuestrasaceite"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.') 
 
    def getFCyCC(self):
        '''Obtener la tabla fcycc.'''
        url = self.api_base+"fccycc"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getEstadoMotor(self):
        '''Obtener la tabla estadomotor.'''
        url = self.api_base+"estadomotor"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getConsolidadoHorometroFaena(self, faena):
        '''Obtener la tabla consolidado_horometro.'''
        url = self.api_base+"consolidadohorometro/faenas/"+faena
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getCodigosFallaSemanal(self):
        '''Obtener la tabla codigosfallasemanal.'''
        url = self.api_base+"codigosfallasemanal"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')      


    def getCodigosFallaSemanalFaena(self, faena):
        '''Obtener la tabla codigosfallasemanal por faena'''
        url = self.api_base+"codigosfallasemanal/faenas/"+faena
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')



    def getCodigosFallaSemanalFaenaFecha(self, faena, fecha):
        '''Obtener la tabla codigosfallasemanal por faena y fecha'''
        url = self.api_base+"codigosfallasemanal/faenas/"+faena+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getCodigosFallaSemanalentreFechas(self, faena, fechainicial, fechafinal):
        '''Obtener la tabla codigosfallasemanal.'''
        url = self.api_base+"codigosfallasemanal/faenas/"+faena+"/fecha/"+fechainicial
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            if (len(df)>0):
                df=df.loc[df['timestamplaston']<fechafinal]
            else:
                df=pd.DataFrame()    
            return df
        else:
            raise NameError('Error en la descarga de datos.')  

    def getCodigosFallaSemanalDia(self, fechainicial):
        '''Obtiene la data de codigo de fallas para el dia indicado'''
        periodoInicial=[' 00:00:00',  ' 03:00:00', ' 06:00:00',  ' 09:00:00', ' 12:00:00', ' 15:00:00',  ' 18:00:00', ' 21:00:00']
        periodoFinal=[' 03:00:00',  ' 06:00:00',  ' 09:00:00', ' 12:00:00', ' 15:00:00',  ' 18:00:00', ' 21:00:00', ' 23:59:59']
      
        df=pd.DataFrame()
        for periodo in periodoInicial:
            pos=periodoInicial.index(periodo)
            dfTemp = self.getCodigosFallaSemanalPeriodo(fechainicial+periodo, fechainicial+periodoFinal[pos])
            df = pd.concat([df, dfTemp], ignore_index=True)
        return df      

    def getCodigosFallaSemanalPeriodo(self, fechainicial, fechafinal):
        '''Obtener la tabla codigosfallasemanal.'''
        url = self.api_base+"codigosfallasemanal/fechas/"+fechainicial+"/"+fechafinal
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            if (len(df)>0):
                df=df.loc[df['timestamplaston']<fechafinal]
            else:
                df=pd.DataFrame()    
            return df
        else:
            raise NameError('Error en la descarga de datos.')  

    def getCodigosFallaSemanalCodigoEntreFechas(self, codigo, fechainicial, fechafinal):
        '''Obtener la tabla codigosfallasemanal por faena y fecha'''
        url = self.api_base+"codigosfallasemanal/codigos/"+codigo+"/fechas/"+fechainicial+"/"+fechafinal
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getCodigosFallaSemanalCodigoFecha(self, codigo, fecha):
        '''Obtener la tabla codigosfallasemanal por faena y fecha'''
        url = self.api_base+"codigosfallasemanal/codigos/"+codigo+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.') 


    def getAfoSemanal(self):
        '''Obtener la tabla afosemanal.'''
        url = self.api_base+"afosemanal"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getAfoSemanalentreFechas(self, fechainicial, fechafinal):
        '''Obtener la tabla afosemanal.'''
        url = self.api_base+"afosemanal/fechas/"+fechainicial+"/"+fechafinal
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')


    def getAfoConsolidado(self):
        '''Obtener la tabla afoconsolidado.'''
        url = self.api_base+"afoconsolidado"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text,  dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getAfoConsolidadoFaena(self, faena):
        '''Obtener la tabla afoconsolidado por faenas.'''
        url = self.api_base+"afoconsolidado/faenas/"+faena
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text,  dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')


    def getScaaLimitesFaenaFlota(self, faena, flota):
        '''Obtener la tabla scaalimites.'''
        url = self.api_base+"scaalimites/faenas/"+faena+"/flotas/"+flota
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:     
            df=pd.read_json(response.text)  
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getScaaLimitesFaena(self, faena):
        '''Obtener la tabla scaalimites.'''
        url = self.api_base+"scaalimites/faenas/"+faena
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:     
            df=pd.read_json(response.text)  
            df.drop('id',  inplace=True, axis=1)
            df=df.loc[df['estado']=='Actual'].reset_index(drop=True)
            return df
        else:
            raise NameError('Error en la descarga de datos.')


    def getScaaFaenaFlotaFechaFechaFinal(self, faena, flota, fechainicio, fechafinal):##nuevo
        '''Obtener la data de un equipo en scaa para en la faena. Requiere faena y equipo.'''
        url = self.api_base+"scaa/faenas/"+faena+"/flotas/"+flota+"/fecha/"+fechainicio
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text)
            df=df.loc[df['fechamuestra']<=fechafinal].reset_index(drop=True)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getScaaFaenaFlotaEquipoFecha(self, faena, flota, equipo, fecha):##nuevo
        '''Obtener la data de un equipo en scaa para en la faena. Requiere faena y equipo.'''
        url = self.api_base+"scaa/faenas/"+faena+"/flotas/"+flota+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:     
            df=pd.read_json(response.text)
            df=df.loc[df['equipo']==equipo].reset_index(drop=True)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getScaaFaenaFlotaFecha(self, faena, flota, fecha):
        '''Obtener la data de un equipo en scaa para en la faena. Requiere faena y equipo.'''
        url = self.api_base+"scaa/faenas/"+faena+"/flotas/"+flota+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:     
            df=pd.read_json(response.text).reset_index(drop=True)	
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getScaaFaenaFecha(self, faena,fecha):
        '''Obtener la data de un equipo en scaa para en la faena. Requiere faena y equipo.'''
        url = self.api_base+"scaa/faenas/"+faena+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:     
            df=pd.read_json(response.text).reset_index(drop=True)	
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getAfoConsolidadoFaenaFecha(self, faena, fecha):##nuevo
        '''Obtener la tabla afoconsolidado.'''
        url = self.api_base+"afoconsolidado/faenas/"+faena+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:     
            df=pd.read_json(response.text)  
            df.drop('id',  inplace=True, axis=1)
            df = df[df['alarma']!= "Alertas Prelube"].reset_index(drop=True)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getAfoConsolidadoFaenak95(self, faena, flota):##nuevo
        '''Obtener la tabla afoconsolidado.'''
        url = self.api_base+"afoconsolidado/faenas/"+faena+"/flotas/"+flota
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:     
            df=pd.read_json(response.text)  
            df.drop('id',  inplace=True, axis=1)
            df = df[df['alarma']!= "Alertas Prelube"].reset_index(drop=True)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getDbmFaenaFechaFilter(self, faena,fecha):

        """
        Devuelve la informacion de DBM para la faena indicada desde la fecha. Se considera fecha de inicio como parametro de busqueda.
        :param faena: faena a buscar
        :type faena: string
        :param fecha: fecha de inicio de la intervencion desde la que se busca
        :type fecha: str
        :return: Dataframe con la informacion requerida
        :rtype: dataframe
        """
        url = self.api_base+"dbm/faenas/"+faena+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text)
            df=df.loc[(df['elemento']=='Filtro primario')|(df['elemento']=='Filtro secundario') & (df['solucion']=='Cambiado_Nuevo')].reset_index(drop=True)
            return df
        else:
            raise NameError('Error en la descarga de datos.') 
            
    def getConsolidadoHorometroFaena(self, faena):
        """
        Obtencion los horometros por faena

        :param faena: faena de la cual se quiere descargar los horometros.
        :return: DataFrame horometros por faena.
        :rtype: Dataframe
        """
        url = self.api_base+"consolidadohorometro/faenas/"+faena
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str, 'unidad':'str', 'unidad':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')
            
    def getDbmFaenaFechaAceite(self, faena,fecha):

        """
        Devuelve la informacion de DBM para la faena indicada desde la fecha. Se considera fecha de inicio como parametro de busqueda.
        :param faena: faena a buscar
        :type faena: string
        :param fecha: fecha de inicio de la intervencion desde la que se busca
        :type fecha: str
        :return: Dataframe con la informacion requerida
        :rtype: dataframe
        """
        url = self.api_base+"dbm/faenas/"+faena+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text)
#             df=df.loc[(df['categoria']=='Fluido')|(df['sistema']=='Fluido')|((df['elemento']=='Aceite Motor')&(df['solucion']=='Cambio'))].reset_index(drop=True)
            df=df.loc[(df['elemento']=='Aceite Motor')&(df['solucion']=='Cambio')].reset_index(drop=True)
            return df
        else:
            raise NameError('Error en la descarga de datos.')
            
    def getDbmFaenaFechaMPs(self, faena,fecha):

        """
        Devuelve la informacion de DBM para la faena indicada desde la fecha. Se considera fecha de inicio como parametro de busqueda.
        :param faena: faena a buscar
        :type faena: string
        :param fecha: fecha de inicio de la intervencion desde la que se busca
        :type fecha: str
        :return: Dataframe con la informacion requerida
        :rtype: dataframe
        """
        url = self.api_base+"dbm/faenas/"+faena+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text)
            df=df.loc[(df['tipo']=='MP')&(df['categoria']=='Inicial')].reset_index(drop=True)
            df=df.loc[(df['sintoma']=='250')|(df['sintoma']=='500')|(df['sintoma']=='1000')].reset_index(drop=True)
            df=df.sort_values(['faena','flota','unidad','fechainicio'], ascending=[1,1,1,1]).reset_index(drop=True)
            return df
        else:
            raise NameError('Error en la descarga de datos.')


    def getMonitoreoAutomaticoFaenaFecha(self, faena, fecha):
        """
        Obtencion del informe base segun la faena.
        :param faena: faena de la cual se quiere descargar el informe diario.
        :return: DataFrame informe diario de los ultimos 7 dias.
        :rtype: Dataframe
        """
        fecha=dt.strptime(fecha, '%Y-%m-%d')
        df=pd.DataFrame()
        dfTemp=self.getMonitoreoAutomaticoFaenaPeriodo(faena,fecha)
        df=pd.concat([df,dfTemp], ignore_index=True)
        return df

    def getMonitoreoAutomaticoFaenaPeriodo(self, faena, fecha):
        """
        Obtiene el informe diario para un dia y faena en particular.

        :param faena:  faena a obtener.
        :type faena: String
        :param fecha: fecha a obtener. formato yyyy-mm-ddd
        :type fecha: String
        :return: Dataframe informe diario dia.
        :rtype: Dataframe
        """
        periodoInicial=[' 00:00:00',  ' 02:00:00', ' 04:00:00',  ' 06:00:00', ' 08:00:00', ' 10:00:00',  ' 12:00:00',  ' 14:00:00',  ' 16:00:00',  ' 18:00:00', ' 20:00:00', ' 22:00:00']
        periodoFinal=[' 02:00:00',  ' 04:00:00',  ' 06:00:00', ' 08:00:00', ' 10:00:00',  ' 12:00:00', ' 14:00:00', ' 16:00:00', ' 18:00:00', ' 20:00:00', ' 22:00:00', ' 23:59:59']
        df=pd.DataFrame()
        for periodo in periodoInicial:
            pos=periodoInicial.index(periodo)
            # print(periodo, pos)
            url = self.api_base+"backupparametros/informediario/faenas/"+faena+'/fechas/'+fecha+periodo+'/'+fecha+periodoFinal[pos]
            payload={}
            headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
            response = requests.request("GET", url, headers=headers, data=payload)
            # print(url)
            if response.status_code == 200:
                dfTemp=pd.read_json(response.text, dtype={'flota':'str','flota':str, 'unidad':'str', 'unidad':str})
                if len(dfTemp) > 0:
                    df=pd.concat([df,dfTemp], ignore_index=True)
                    del dfTemp
            else:
                raise NameError('Error en la descarga de datos. '+ str(response.status_code))

        return df

    def getInformeDiarioLbrCmz(self, faena):
        """
        Obtencion del informe base LBR - CMZ. 

        :param faena: faena de la cual se quiere descargar el informe diario.
        :return: DataFrame informe diario.
        :rtype: Dataframe
        """
        today=dt.today()
        fechainicial=today-timedelta(days=7)
        fechas=[]
        while fechainicial<today:
            fechas.append(fechainicial.strftime('%Y-%m-%d'))
            fechainicial=fechainicial+timedelta(days=1)
        df=pd.DataFrame()
        bar = tqdm(total = 7)
        for fecha in fechas:
            # print(fecha)
            dfTemp=self.getInformeDiarioLbrCmzFaenaFecha(faena,fecha)
            df=pd.concat([df,dfTemp], ignore_index=True)
            bar.update(1)
        bar.close()
        return df



    def getInformeDiarioLbrCmzFaenaFecha(self, faena, fecha):
        """
        Obtiene el informe diario LBR - CMZ para un dia y faena en particular.

        :param faena:  faena a obtener.
        :type faena: String
        :param fecha: fecha a obtener. formato yyyy-mm-ddd
        :type fecha: String
        :return: Dataframe informe diario dia LBR - CMZ.
        :rtype: Dataframe
        """
        periodoInicial=[' 00:00:00',  ' 03:00:00', ' 06:00:00',  ' 09:00:00', ' 12:00:00', ' 15:00:00',  ' 18:00:00', ' 21:00:00', ' 23:00:00']
        periodoFinal=[' 03:00:00',  ' 06:00:00',  ' 09:00:00', ' 12:00:00', ' 15:00:00',  ' 18:00:00', ' 21:00:00', ' 23:00:00', ' 23:59:59']
        df=pd.DataFrame()
        for periodo in periodoInicial:
            pos=periodoInicial.index(periodo)
            url = self.api_base+"backupparametros/informediariolbrcmz/faenas/"+faena+'/fechas/'+fecha+periodo+'/'+fecha+periodoFinal[pos]
            payload={}
            headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
            response = requests.request("GET", url, headers=headers, data=payload)
            if response.status_code == 200:
                dfTemp=pd.read_json(response.text, dtype={'flota':'str','flota':str, 'unidad':'str', 'unidad':str})
                if len(dfTemp) > 0:
                    df=pd.concat([df,dfTemp], ignore_index=True)
                    del dfTemp
            else:
                raise NameError('Error en la descarga de datos.')

        return df

    def getInformeDiarioCranckcase(self, faena):
        """
        Obtencion del informe base LBR - CMZ.

        :param faena: faena de la cual se quiere descargar el informe diario.
        :return: DataFrame informe diario.
        :rtype: Dataframe
        """
        today=dt.today()
        fechainicial=today-timedelta(days=14)
        fechas=[]
        while fechainicial<today:
            fechas.append(fechainicial.strftime('%Y-%m-%d'))
            fechainicial=fechainicial+timedelta(days=1)
        df=pd.DataFrame()
        bar = tqdm(total = 14)
        for fecha in fechas:
            dfTemp=self.getInformeDiarioCranckcaseFaenaFecha(faena,fecha)
            df=pd.concat([df,dfTemp], ignore_index=True)
            bar.update(1)
        bar.close()
        return df


    def getInformeDiarioCranckcaseFaenaFecha(self, faena, fecha):
        """
        Obtiene el informe diario LBR - CMZ para un dia y faena en particular.

        :param faena:  faena a obtener.
        :type faena: String
        :param fecha: fecha a obtener. formato yyyy-mm-ddd
        :type fecha: String
        :return: Dataframe informe diario dia LBR - CMZ.
        :rtype: Dataframe
        """
        periodoInicial=[' 00:00:00',  ' 03:00:00', ' 06:00:00',  ' 09:00:00', ' 12:00:00', ' 15:00:00',  ' 18:00:00', ' 21:00:00', ' 23:00:00']
        periodoFinal=[' 03:00:00',  ' 06:00:00',  ' 09:00:00', ' 12:00:00', ' 15:00:00',  ' 18:00:00', ' 21:00:00', ' 23:00:00', ' 23:59:59']
        df=pd.DataFrame()
        for periodo in periodoInicial:
            pos=periodoInicial.index(periodo)
            url = self.api_base+"backupparametros/informediariocranckcase/faenas/"+faena+'/fechas/'+fecha+periodo+'/'+fecha+periodoFinal[pos]
            payload={}
            headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
            response = requests.request("GET", url, headers=headers, data=payload)
            if response.status_code == 200:
                dfTemp=pd.read_json(response.text, dtype={'flota':'str','flota':str, 'unidad':'str', 'unidad':str})
                if len(dfTemp) > 0:
                    df=pd.concat([df,dfTemp], ignore_index=True)
                    del dfTemp
            else:
                raise NameError('Error en la descarga de datos.')

        return df


    def getInformeDiarioCoolant(self, faena):
        """
        Obtencion del informe base LBR - CMZ.

        :param faena: faena de la cual se quiere descargar el informe diario.
        :return: DataFrame informe diario.
        :rtype: Dataframe
        """
        today=dt.today()
        fechainicial=today-timedelta(days=15)
        fechas=[]
        while fechainicial<today:
            fechas.append(fechainicial.strftime('%Y-%m-%d'))
            fechainicial=fechainicial+timedelta(days=1)
        df=pd.DataFrame()
        bar = tqdm(total = 15)
        for fecha in fechas:
            dfTemp=self.getInformeDiarioCoolantFaenaFecha(faena,fecha)
            df=pd.concat([df,dfTemp], ignore_index=True)
            bar.update(1)
        bar.close()
        return df

    def getInformeDiarioCoolantFaenaFecha(self, faena, fecha):
        """
        Obtiene el informe diario LBR - CMZ para un dia y faena en particular.

        :param faena:  faena a obtener.
        :type faena: String
        :param fecha: fecha a obtener. formato yyyy-mm-ddd
        :type fecha: String
        :return: Dataframe informe diario dia LBR - CMZ.
        :rtype: Dataframe
        """
        periodoInicial=[' 00:00:00',  ' 03:00:00', ' 06:00:00',  ' 09:00:00', ' 12:00:00', ' 15:00:00',  ' 18:00:00', ' 21:00:00', ' 23:00:00']
        periodoFinal=[' 03:00:00',  ' 06:00:00',  ' 09:00:00', ' 12:00:00', ' 15:00:00',  ' 18:00:00', ' 21:00:00', ' 23:00:00', ' 23:59:59']
        df=pd.DataFrame()
        for periodo in periodoInicial:
            pos=periodoInicial.index(periodo)
            url = self.api_base+"backupparametros/informediariocoolant/faenas/"+faena+'/fechas/'+fecha+periodo+'/'+fecha+periodoFinal[pos]
            payload={}
            headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
            response = requests.request("GET", url, headers=headers, data=payload)
            if response.status_code == 200:
                dfTemp=pd.read_json(response.text, dtype={'flota':'str','flota':str, 'unidad':'str', 'unidad':str})
                if len(dfTemp) > 0:
                    df=pd.concat([df,dfTemp], ignore_index=True)
                    del dfTemp
            else:
                raise NameError('Error en la descarga de datos.')

        return df

    def getInformeDiarioQsk95(self, faena):
        """
        Obtencion del informe base LBR - CMZ.

        :param faena: faena de la cual se quiere descargar el informe diario.
        :return: DataFrame informe diario.
        :rtype: Dataframe
        """
        today=dt.today()
        fechainicial=today-timedelta(days=60)
        fechas=[]
        while fechainicial<today:
            fechas.append(fechainicial.strftime('%Y-%m-%d'))
            fechainicial=fechainicial+timedelta(days=1)
        df=pd.DataFrame()
        bar = tqdm(total = 60)
        for fecha in fechas:
            dfTemp=self.getInformeDiarioQsk95FaenaFecha(faena,fecha)
            df=pd.concat([df,dfTemp], ignore_index=True)
            bar.update(1)
        bar.close()    
        return df

    def getInformeDiarioQsk95FaenaFecha(self, faena, fecha):
        """
        Obtiene el informe diario LBR - CMZ para un dia y faena en particular.

        :param faena:  faena a obtener.
        :type faena: String
        :param fecha: fecha a obtener. formato yyyy-mm-ddd
        :type fecha: String
        :return: Dataframe informe diario dia LBR - CMZ.
        :rtype: Dataframe
        """
        periodoInicial=[' 00:00:00',  ' 03:00:00', ' 06:00:00',  ' 09:00:00', ' 12:00:00', ' 15:00:00',  ' 18:00:00', ' 21:00:00', ' 23:00:00']
        periodoFinal=[' 03:00:00',  ' 06:00:00',  ' 09:00:00', ' 12:00:00', ' 15:00:00',  ' 18:00:00', ' 21:00:00', ' 23:00:00', ' 23:59:59']
        df=pd.DataFrame()
        for periodo in periodoInicial:
            pos=periodoInicial.index(periodo)
            url = self.api_base+"backupparametros/informediarioqsk95/faenas/"+faena+'/fechas/'+fecha+periodo+'/'+fecha+periodoFinal[pos]
            payload={}
            headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
            response = requests.request("GET", url, headers=headers, data=payload)
            if response.status_code == 200:
                dfTemp=pd.read_json(response.text, dtype={'flota':'str','flota':str, 'unidad':'str', 'unidad':str})
                if len(dfTemp) > 0:
                    df=pd.concat([df,dfTemp], ignore_index=True)
                    del dfTemp
            else:
                raise NameError('Error en la descarga de datos.')

        return df

    def getInformeDiarioFitFaena(self, faena, dias):
        """
        Obtencion del informe base fit

        :param faena: faena de la cual se quiere descargar el informe diario.
        :param dias: dias a buscar
        :return: DataFrame informe diario.
        :rtype: Dataframe
        """
        today=dt.today()
        fechainicial=today-timedelta(days=dias)
        fechas=[]
        while fechainicial<today:
            fechas.append(fechainicial.strftime('%Y-%m-%d'))
            fechainicial=fechainicial+timedelta(days=1)
        df=pd.DataFrame()
        bar = tqdm(total = dias)
        for fecha in fechas:
            dfTemp=self.getInformeDiarioFitFaenaFecha(faena,fecha)
            df=pd.concat([df,dfTemp], ignore_index=True)
            bar.update(1)
        bar.close()    
        return df

    def getInformeDiarioDosFaena(self, faena):
        """
        Obtencion del informe diario. 

        :param faena: faena de la cual se quiere descargar los equipos desconectados.
        :param periodo: periodo para la cual quiere descargar los equipos desconectados.
        :return: DataFrame informe diario.
        :rtype: Dataframe
        """
        url = self.api_base+"informediario/faenasdos/"+faena
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'unidad':'str', 'flota':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getEquiposDesconectados(self, faena, periodo):
        """
        Obtencion los equipos desconectados. 

        :param faena: faena de la cual se quiere descargar los equipos desconectados.
        :param periodo: periodo para la cual quiere descargar los equipos desconectados.
        :return: DataFrame informe diario.
        :rtype: Dataframe
        """
        url = self.api_base+"equiposdesconectados/faenas/"+faena+"/periodo/"+periodo
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str, 'unidad':'str', 'unidad':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getScaaFaenaFlota(self, faena, flota):
        """
        Obtener la tabla scaa por faena.

        :param faena: nombre de la faena a obtener.
        :return: datos de aceite de la faena.
        :rtype: DataFrame
        """
        url = self.api_base+"scaa/faenas/"+faena+"/flotas/"+flota
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str,'equipo':'str','equipo':str})
            df.loc[(df['faena']=='Escondida')&(df['flota']=='930 E'), 'diluc']=0
            df.loc[(df['faena']=='Escondida')&(df['flota']=='960 E'), 'diluc']=0
            df.loc[(df['faena']=='Escondida')&(df['flota']=='930 E'), 'ptoinfl']=0
            df.loc[(df['faena']=='Escondida')&(df['flota']=='960 E'), 'ptoinfl']=0
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getScaaFaenaEquipo(sefl, faena, equipo):
        """
        Obtener la tabla scaa por faena desde la fecha indicada, filtrando por un equipo en particular.

        :param faena: nombre de la faena a obtener.
        :type faena: string
        :param equipo: equipo del que se entregaran los resultados
        :type equipo: string
        :return: datos de aceite del equipo en la faena indicada.
        :rtype: DataFrame
        """    
        url = self.api_base+"scaa/faenas/"+faena+"/equipos/"+equipo
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype = {'flota':'str', 'flota':'str', 'equipo':'str', 'equipo':str, 'esn':'str', 'esn':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getDbmFaenaFecha(self, faena, fecha):
        """
        Devuelve la informacion de DBM para la faena indicada desde la fecha. Se considera fecha de inicio como parametro de busqueda.
        :param faena: faena a buscar
        :type faena: string
        :param fecha: fecha de inicio de la intervencion desde la que se busca
        :type fecha: str
        :return: Dataframe con la informacion requerida
        :rtype: dataframe
        """
        url = self.api_base+"dbm/faenas/"+faena+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str, 'equipo':'str', 'equipo':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getDbmFaenaFlotaEquipoFecha(self, faena, flota, equipo, fecha):
        """
        Devuelve la informacion de DBM para la faena indicada desde la fecha. Se considera fecha de inicio como parametro de busqueda.
        :param faena: faena a buscar
        :type faena: string
        :param fecha: fecha de inicio de la intervencion desde la que se busca
        :type fecha: str
        :return: Dataframe con la informacion requerida
        :rtype: dataframe
        """
        url = self.api_base+"dbm/faenas/"+faena+"/flotas/"+flota+"/equipos/"+equipo+"/fecha/"+fecha
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str'})
            return df
        else:
            raise NameError('Error en la descarga de datos.')


    def getDbmRankingUno(self, faena, equipo, fechainicio):
        '''Obtener la fecha maxima segun flota en scaa para cada faena.'''
        url = self.api_base+"dbm/informerankinguno/faenas/"+faena+"/equipos/"+equipo+"/fecha/"+fechainicio+' 00:00:00'
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getDbmRankingDos(self, faena, equipo, fechainicio):
        '''Obtener la fecha maxima segun flota en scaa para cada faena.'''
        url = self.api_base+"dbm/informerankingdos/faenas/"+faena+"/equipos/"+equipo+"/fecha/"+fechainicio+' 00:00:00'
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getDbmRankingTres(self, faena, equipo, fechainicio):
        '''Obtener la fecha maxima segun flota en scaa para cada faena.'''
        url = self.api_base+"dbm/informerankingtres/faenas/"+faena+"/equipos/"+equipo+"/fecha/"+fechainicio+' 00:00:00'
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getDbmRankingCuatro(self, faena, equipo, fechainicio):
        '''Obtener la fecha maxima segun flota en scaa para cada faena.'''
        url = self.api_base+"dbm/informerankingcuatro/faenas/"+faena+"/equipos/"+equipo+"/fecha/"+fechainicio+' 00:00:00'
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getDbmFaenaEquipo(self, faena, equipo):
        """Obtener la data de dbm segun faena y equipo en dbn.
        :param faena: faena buscada.
        :type faena: str
        :param equipo: equipo buscado.
        :type equipo: str
        :return: dataframe con la data solicitada
        :rtype: DataFrame
        """
        url = self.api_base+"dbm/faenas/"+faena+"/equipos/"+equipo
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str, 'unidad':'str', 'unidad':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getDbmComponenteSubsistema(self, subsistema, fechainicio, fechafin):
        """Obtener el historial de componentes solicitado segun la subsistema provisto.
        :param subsistema: subsistema del componente buscado
        :type subsistema: str
        :param fechainicio: fecha desde la cual comienza la busqueda. formato yyyy-mm-dd
        :type fechainicio: str
        :param fechafin: fecha desde la cual comienza la busqueda. formato yyyy-mm-dd
        :type fechafin: str
        :return: DataFrame con la informacion de componente en el listado de elementoId
        :rtype:
        """                
        url = self.api_base+"dbm/componentes/subsistema/"+fechainicio+"/"+fechafin+"/"+subsistema
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str, 'unidad':'str', 'unidad':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getDbmComponentePosicionSubsistema(self, posicionsubsistema, fechainicio, fechafin):
        """Obtener el historial de componentes solicitado segun la posicionsubsistema provisto.
        :param posicionsubsistema: posicionsubsistema del componente buscado
        :type posicionsubsistema: str
        :param fechainicio: fecha desde la cual comienza la busqueda. formato yyyy-mm-dd
        :type fechainicio: str
        :param fechafin: fecha desde la cual comienza la busqueda. formato yyyy-mm-dd
        :type fechafin: str
        :return: DataFrame con la informacion de componente en el listado de elementoId
        :rtype:
        """        
        url = self.api_base+"dbm/componentes/posicionsubsistema/"+fechainicio+"/"+fechafin+"/"+posicionsubsistema
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str, 'unidad':'str', 'unidad':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getDbmComponenteIdElemento(self, idelemento, fechainicio, fechafin):
        """Obtener el historial de componentes solicitado segun el Idelemento provisto.
        :param idelemento: idelemento del componente buscado
        :type idelemento: int32
        :param fechainicio: fecha desde la cual comienza la busqueda. formato yyyy-mm-dd
        :type fechainicio: str
        :param fechafin: fecha desde la cual comienza la busqueda. formato yyyy-mm-dd
        :type fechafin: str
        :return: DataFrame con la informacion de componente en el listado de elementoId
        :rtype:
        """
        url = self.api_base+"dbm/componentes/idelemento/"+fechainicio+"/"+fechafin+"/"+idelemento
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str, 'unidad':'str', 'unidad':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getDbmComponenteElementoId(self, elementoId, fechainicio, fechafin):
        """Obtener el historial de componentes solicitado segun el listado de elementoId provisto.
        :param elementoId: elemento id del componente buscado
        :type elementoId: listado de elementos
        :param fechainicio: fecha desde la cual comienza la busqueda. formato yyyy-mm-dd
        :type fechainicio: str
        :param fechafin: fecha desde la cual comienza la busqueda. formato yyyy-mm-dd
        :type fechafin: str
        :return: DataFrame con la informacion de componente en el listado de elementoId
        :rtype: DataFrame
        """
        url = self.api_base+"dbm/componentes/elementoid/"+fechainicio+"/"+fechafin+"/"+elementoId
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'flota':'str', 'flota':str, 'unidad':'str', 'unidad':str})
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getScaaLimitesActual(self):
        """
        Regresa los limites de scaa actuales
        :return: los limites scaa actualizados.
        :rtype: dataframe
        """
        url = self.api_base+"scaalimites"
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df=df.loc[df['estado']=='Actual']
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')

    def getScaaLimitesFaenaEstado(self, faena, estado):
        """
        Obtiene los limites de scaa para la faena segun el estado.
        :param faena: faena deseada
        :type faena: str
        :param estado: estado deseado
        :type estado: str
        :return: el listado de los limites segun estado para la faena deseada.
        :rtype: DataFrame
        """
        url = self.api_base+"scaalimites/faenas/"+faena+"/estado/"+estado
        payload={}
        headers = { 'Authorization': 'Bearer '+self.token, 'Cookie': 'JSESSIONID=CB86E0E21029DE39FC28FC2FF43ECB07'}
        response = requests.request("GET", url, headers=headers, data=payload)
        if response.status_code == 200:
            df=pd.read_json(response.text, dtype={'esn':str, 'esn':'str', 'flota':str, 'flota':'str', 'equipo':'str', 'equipo':str})
            df.drop('id',  inplace=True, axis=1)
            return df
        else:
            raise NameError('Error en la descarga de datos.')
