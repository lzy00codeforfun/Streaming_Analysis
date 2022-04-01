<template>
  <div class="hello">
    <h4>{{ msg }}</h4>

    <img :src= "require('../assets/' + this.imagePath)">
  </div>
</template>

<script>

import axios from "axios";

export default {
  name: 'HashtagRank',
  data() {
    return {
      imagePath: "WordCloud.png",
    };
  },
  props: {
    msg: String
  },
  methods: {
    getRank() {
      const path = 'http://localhost:5000/hashtag_rank';
      axios.get(path)
        .then((res) => {
          this.imagePath = res.data["file_name"];
        })
        .catch((error) => {
          // eslint-disable-next-line
          console.error(error);
        });
    },
  },
  created() {
    setInterval(this.getRank(), 1000);
    //this.getRank();
  },
  beforeUnmount(){
    clearInterval(this.imagePath);
  }
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
h4 {
  margin: -10px 0 0;
}

</style>
