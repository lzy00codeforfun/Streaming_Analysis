<template>
  <img src="./assets/DataCube.png"
      height="100"
  >
  <div>
  </div>
  <HashtagRank msg="Welcome to Your Vue.js App"/>
  <SearchTool/>
</template>

<script>
import HashtagRank from './components/HashtagRank.vue'
import SearchTool from './components/SearchTool.vue'

export default {
  name: 'App',
  components: {
    HashtagRank,
    SearchTool
  }
}
</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: -10px;
}
</style>
