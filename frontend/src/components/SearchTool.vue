<template>
  <div class="hello">

      
    <!-- <input v-model="message" placeholder="Search Content">
    <button v-on:click="search">Search</button> -->

    <el-row :gutter="24">
        <el-col :span="14" :offset="5">
            <el-input v-model="message"
              v-on:keyup.enter="search"
              placeholder="Search Something"
              :prefix-icon="Search">
            </el-input>
        </el-col>

        <el-col :span="1">
            <el-button v-on:click="search">
                Search
            </el-button>
        </el-col>
    </el-row>

    <el-row :gutter="24">
        <el-col :span="14" :offset="5">
            <el-table :data="messageList" style="width: 100%">
            <el-table-column prop="time" label="Time" width="240" />
            <el-table-column prop="content" label="Content" />
            <el-table-column prop="hashtag" label="Hashtag" width="120"/>
            </el-table>
        </el-col>
    </el-row>
  
  </div>
</template>
<script>

import axios from "axios";

export default {
  name: 'SearchTool',
  data() {
    return {
      message: "",
      messageList: [],
    };
  },

  methods: {
    search() {
      const url = 'http://localhost:5000/search';
      const params = {"content": this.message};
      axios.get(url, {params: params})
        .then((res) => {
          this.messageList = res.data["message_list"];
        })
        .catch((error) => {
          console.error(error);
        });
    },
  },
}
</script>

<!-- Add "scoped" attribute to limit CSS to this component only -->
<style scoped>
h4 {
  margin: -10px 0 0;
}

</style>
