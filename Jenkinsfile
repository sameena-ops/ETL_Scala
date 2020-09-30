 pipeline {
   agent any

   stages {
      stage('checkout') {
         steps {
            checkout([$class: 'GitSCM', branches: [[name: '*/master']],
            doGenerateSubmoduleConfigurations: false, extensions: [],
            submoduleCfg: [],
            userRemoteConfigs: [[url: 'https://github.com/sameena-ops/ETL_Scala.git']]])
         }
      }

	    stage('Unit Tests') {
         steps {
		 echo "Running tests on the data provided"
            sh 'java -Dsbt.log.noformat=true -jar /var/jenkins_home/tools/org.jvnet.hudson.plugins.SbtPluginBuilder_SbtInstallation/1.3.13/bin/sbt-launch.jar "testOnly extract.ValidateNoDuplicateStationEntries"'}
			}

      stage('Main ExtractTransform') {
         steps {
		 echo "Extracting the required data"
            sh 'java -Dsbt.log.noformat=true -jar /var/jenkins_home/tools/org.jvnet.hudson.plugins.SbtPluginBuilder_SbtInstallation/1.3.13/bin/sbt-launch.jar "runMain ExtractTransformJob /var/jenkins_home/data/2017.csv  /var/jenkins_home/data/ghcnd-stations.txt"'}
			}


      stage('Integration Tests') {
         steps {
		 echo "Running integration tests on the transformed data"
            sh 'java -Dsbt.log.noformat=true -jar /var/jenkins_home/tools/org.jvnet.hudson.plugins.SbtPluginBuilder_SbtInstallation/1.3.13/bin/sbt-launch.jar "testOnly it.ValidateLatLonMappedCorrectlyForStations"'}
			}
       stage('Main LoadData') {
         steps {
		 echo "Run the tests to check the loaded data"
            sh 'java -Dsbt.log.noformat=true -jar /var/jenkins_home/tools/org.jvnet.hudson.plugins.SbtPluginBuilder_SbtInstallation/1.3.13/bin/sbt-launch.jar "runMain LoadJob /var/jenkins_home/data/2017.csv  /var/jenkins_home/data/ghcnd-stations.txt"'}
			}
   }
   }
