node {

    stage 'Build'

    withMaven(maven: 'Maven') {
        sh "mvn -U clean deploy"
    }
}
