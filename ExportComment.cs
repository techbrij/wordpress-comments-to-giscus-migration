using System.Data;
using MySql.Data.MySqlClient;

using GraphQL;
using GraphQL.Client.Http;
using GraphQL.Client.Serializer.Newtonsoft;

public class ExportComment
{

    public static string github_token = "github_pat_11A0000000000000000000000000zE"; // Generated via GitHub Profile Setting
    public static string github_repo_id = "R_kg0000000000"; // Repo ID from Giscus generated code
    public static string github_category_id = "DIC_kw00000000000";  // General Discussion from Giscus generated code

    public class Discussion
    {
        public string id;
    }
    public class CreateDiscussion
    {
        public Discussion discussion;
    }

    public class DiscussionRoot
    {
        public CreateDiscussion createDiscussion;
    }

    public class Comment
    {
        public string id;
    }

    public class addDiscussionComment
    {
        public Comment comment;
    }

    public class CommentRoot
    {
        public addDiscussionComment addDiscussionComment;
    }


    public static async Task Export(string connectionString)
    {
        using (var client = new GraphQLHttpClient("https://api.github.com/graphql", new NewtonsoftJsonSerializer()))
        {

            client.HttpClient.DefaultRequestHeaders.Add("Authorization", $"Bearer {github_token}");

            // Save Posts
            var dtPosts = GetPostsToExport(connectionString);
            var i = 0;
            foreach(DataRow row in dtPosts.Rows)
            {
                var id = (UInt64)row["ID"];
                var slug = (string) row["slug"];
                var postTitle = (string) row["post_title"] ?? "";
                var postExcerpt = (string) row["post_excerpt"] ?? "";
                var url = "//techbrij.com/"+ slug;
                var postBody ="### "+ postTitle + " \n\n  "+ postExcerpt +" \n\n [Read full article](" +url +") \n ";

                var discussionID = await CreateGitHubDiscussion(client, github_repo_id, slug, postBody, github_category_id);
                SaveGithubPostID(connectionString,id,discussionID);   
                i++;         
            }

            // for mapping github id
            var dtCommentsAll = GetAllComments(connectionString);
            var commentGithubMap = new Dictionary<UInt64, string>();
            var commentParentMap = new Dictionary<UInt64, UInt64>();
            foreach (DataRow row in dtCommentsAll.Rows)
            {
                var gcid = row["github_comment_ID"] == DBNull.Value ? "" : (string)row["github_comment_ID"];
                commentGithubMap.Add((UInt64)row["comment_ID"], gcid);
                commentParentMap.Add((UInt64)row["comment_ID"], (UInt64)row["comment_parent"]);
            }

            // Save Comments
            var dtComments = GetComments(connectionString);       
            i = 0;
            foreach (DataRow row in dtComments.Rows)
            {

                var commentId = (UInt64)row["comment_ID"];
                var githubPostID = (string)row["github_post_ID"];
                var commentBody = (string)row["comment_content"];
                var commentAuthor = (string)row["comment_author"];
                var commentDate = (DateTime)row["comment_date"];
                var commentParent = (UInt64)row["comment_parent"];

                var comment = "_**" + commentAuthor + " "+ commentDate.ToString() + "** (Migrated from WordPress)_:  \n\n" + commentBody;
                var githubCommentID = "";
                if (commentParent == null || commentParent == 0)
                {
                    githubCommentID = await CreateGitHubDiscussionComment(client, githubPostID, comment);
                }
                else
                {
                    // As Github discussion 2 levels only so need to get nested comments as 2 level 
                    var parentID = commentParent;
                    while(commentParent > 0) {
                        parentID = commentParent;
                        commentParent = commentParentMap[commentParent];
                    }

                    var parentGithubId = commentGithubMap[parentID];

                    if (!string.IsNullOrEmpty(parentGithubId))
                    {
                       githubCommentID  = await CreateGitHubDiscussionComment(client, githubPostID, comment, parentGithubId);
                    }
                    else{
                        Console.WriteLine("No Parent Comment found for commentId "+ commentId);
                        continue;
                    }
                   
                }
                SaveGithubCommentID(connectionString, commentId, githubCommentID);
                commentGithubMap[commentId] = githubCommentID;
                i++;
                Console.WriteLine($"Added Comment " +  i);                                      
            }
        }
    }

    public static void SaveGithubCommentID(string connectionString, UInt64 commentId, string githubCommentId)
    {
        string sql = $"update db.migration_comments set github_comment_ID = '{githubCommentId}' where comment_ID = {commentId}";
        RunSaveQuery(connectionString, sql);
    }

    public static void SaveGithubPostID(string connectionString, UInt64 postId, string githubPostId)
    {
        string sql = $"update db.migration_comments set github_post_ID = '{githubPostId}' where post_ID = {postId}";
        RunSaveQuery(connectionString, sql);
    }

     public static DataTable GetAllComments(string connectionString)
    {
        string sql = @"SELECT comment_ID,comment_parent, github_comment_ID FROM db.migration_comments";
        return RunGetQuery(connectionString, sql);
    }

    public static DataTable GetComments(string connectionString)
    {
        string sql = @"SELECT * FROM db.migration_comments where github_comment_ID is null and github_post_ID is not null;";
        return RunGetQuery(connectionString, sql);
    }

    public static DataTable GetPostsToExport(string connectionString)
    {

        string sql = @"SELECT ID, post_name as slug, post_title, post_excerpt 
FROM db.wp_posts 
where ID in (SELECT distinct post_ID FROM db.migration_comments where github_post_ID is null)";
        return RunGetQuery(connectionString, sql);
    }

    public static DataTable RunGetQuery(string connectionString, string query)
    {
        using (var con = new MySqlConnection(connectionString))
        {
            string sql = query;
            using (var cmd = new MySqlCommand(sql, con))
            {
                using (var adapter = new MySqlDataAdapter(cmd))
                {
                    var resultTable = new DataTable();
                    adapter.Fill(resultTable);
                    return resultTable;
                }
            }
        }
    }

    public static int RunSaveQuery(string connectionString, string query)
    {
        using (var con = new MySqlConnection(connectionString))
        {
            string sql = query;
            con.Open();
            using (var cmd = new MySqlCommand(sql, con))
            {
                int recordsAffected = cmd.ExecuteNonQuery();
                return recordsAffected;
            }
        }
    }

    public static async Task<string> CreateGitHubDiscussion(GraphQLHttpClient client, string repoId, string title, string body, string categoryId)
    {
        try
        {

            var query = @"
                mutation CreateDiscussion($repositoryId: ID!, $title: String!, $body: String!, $categoryId: ID!) {
                    createDiscussion(input: {
                        repositoryId: $repositoryId,
                        title: $title,
                        body: $body, 
				        categoryId: $categoryId
                    }) {
                        discussion {
                            id
                        }
                    }
                }
            ";

            var variables = new
            {
                repositoryId = repoId,
                title = title,
                body = body,
                categoryId = categoryId
            };

            var request = new GraphQLRequest { Query = query, Variables = variables };

            var response = await client.SendMutationAsync<DiscussionRoot>(request);

            if (response.Errors != null && response.Errors.Count() > 0)
            {
                throw new Exception($"GitHub GraphQL API error: {string.Join(", ", response.Errors.Select(e => e.Message))}");
            }

            var discussionId = response.Data.createDiscussion.discussion.id;
            return discussionId;


        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            throw;
        }

    }

    public static async Task<string> CreateGitHubDiscussionComment(GraphQLHttpClient client, string discussionId, string body, string parentCommentId = null)
    {
        try
        {
            var query = @"
                mutation CreateDiscussionComment($discussionId: ID!, $body: String!, $replyToId: ID) {
                    addDiscussionComment(input: {
                        discussionId: $discussionId, 
						body: $body, 
						replyToId: $replyToId
                    }) { 
                        comment { id }                       
                    }
                }
            ";

            var variables = new
            {
                discussionId = discussionId,
                body = body,
                replyToId = parentCommentId
            };

            var request = new GraphQLRequest { Query = query, Variables = variables };

            var response = await client.SendMutationAsync<CommentRoot>(request);

            if (response.Errors != null && response.Errors.Count() > 0)
            {
                throw new Exception($"GitHub GraphQL API error: {string.Join(", ", response.Errors.Select(e => e.Message))}");
            }

            var commentId = response.Data.addDiscussionComment.comment.id;
            return commentId;

        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
            throw;
        }

    }

}
