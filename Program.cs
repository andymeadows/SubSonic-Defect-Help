using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using SubSonic.Query;
using SubSonic.Extensions;
using SubSonic.Linq.Structure;

namespace SubSonic.Defects
{
    public class SoldTicketData
    {
        public string Movie { get; set; }
        public string Genre { get; set; }
        public string SecondaryGenre { get; set; }
        public decimal TicketPrice { get; set; }
    }

    class Program
    {
        // I'm intentionally not using the values setup here to illustrate
        // that there hasn't been much testing on the  pieces of this code
        // that aren't covered by the basic context that is auto-generated.
        private static readonly SampleDataConnectionStringDB db = new SampleDataConnectionStringDB();

        static void Main(string[] args)
        {
            // Empty the database.
            EmptyDatabase();

            // Insert single items.
            int genreId = SingleInsertGenre(); // This works.
            int secondaryGenreId = SingleInsertSecondaryGenreUsingToInsert(); //  this works.
            int movieId = SingleInsertMovie(genreId, secondaryGenreId); // This works.
            int ticketId = SingleInsertTicket(movieId); // This works.

            // Validate that it works.  Note, these all work here but fail in my larger environment
            //SelectTicketsUsingProjectionAndIDataProvider(movieId); // Works here.  Just not in my other project.
            //SelectTicketsUsingProjectionQueryingAgainstTheSameTableOnJoin(movieId); // Works.
            //SelectTicketsUsingProjectionQueryingAgainstTheSameTableOnJoin2(movieId); // Works.

            SelectUsingContains(new List<string> { "Genre's One and Only", "Genre's Second" }); // Fails.  Un-escaped single quote.

            //// Try batch inserts.
            BatchInsertInto(); // Fails -- parameters.
            BatchToInsert();  // Fails -- parameters.
            BatchInsertOnNonReservedFieldNames(movieId); // Fails -- parameters
        }

        private static void EmptyDatabase()
        {
            Console.WriteLine("Resetting database.");

            new CodingHorror("delete from movies;").Execute();
            new CodingHorror("delete from genres;").Execute();
        }

        #region Single Inserts
        private static int SingleInsertGenre()
        {
            Console.WriteLine("Inserting via single connection with unescaped values.");

            return ExecuteQuery(db.Insert.Into<Genre>(g => g.Name).Values("Genre's One and Only"));
        }

        private static int SingleInsertSecondaryGenreUsingToInsert()
        {
            Console.WriteLine("Inserting via single connection with unescaped values.");

            Genre genre = new Genre
            {
                Name = "Genre's Second."
            };

            return ExecuteQuery(genre.ToInsertQuery<Genre>(db.Provider));
        }

        private static int SingleInsertMovie(int genreId, int secondaryGenreId)
        {
            Console.WriteLine("Inserting a single movie using ToInsertand unescaped values.");

            Movie movie = new Movie
            {
                Name = "National Lampoon's Vacation",
                GenreId = genreId,
                SecondaryGenreId = secondaryGenreId
            };

            return ExecuteQuery(movie.ToInsertQuery<Movie>(db.Provider));
        }

        private static int SingleInsertTicket(int movieId)
        {
            Console.WriteLine("Inserting a ticket purchase.");

            Ticket ticket = new Ticket
            {
                MovieId = movieId,
                Price = 1.00M,
                SoldTo = "Andy Meadows",
                Age = 95
            };

            return ExecuteQuery(ticket.ToInsertQuery<Ticket>(db.Provider));
        }

        #endregion

        #region Selects

        private static void SelectTicketsUsingProjectionAndIDataProvider(int movieId)
        {
            // Select the item.
            Query<Ticket> tickets = new Query<Ticket>(db.Provider);
            Query<Movie> movies = new Query<Movie>(db.Provider);
            Query<Genre> genres = new Query<Genre>(db.Provider);

            var theTicket = (from ticket in tickets
                             join movie in movies
                             on ticket.MovieId equals movie.MovieId
                             join genre in genres
                             on movie.GenreId equals genre.GenreId
                             where ticket.MovieId == movieId
                             select new SoldTicketData
                             {
                                 TicketPrice = ticket.Price,
                                 Genre = genre.Name,
                                 Movie = movie.Name
                             }).FirstOrDefault();
            // Validate values.

            try
            {
                Console.WriteLine(string.Format("Success, sold a {0} ticket to the {1} hit {2}", theTicket.TicketPrice, theTicket.Genre, theTicket.Movie));
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: Projection failed - " + ex.Message);
            }
        }

        private static void SelectTicketsUsingProjectionQueryingAgainstTheSameTableOnJoin(int movieId)
        {
            // Select the item.
            Query<Ticket> tickets = new Query<Ticket>(db.Provider);
            Query<Movie> movies = new Query<Movie>(db.Provider);
            Query<Genre> genres = new Query<Genre>(db.Provider);

            // Validate values.

            try
            {
                var theTicket = (from ticket in tickets
                                 join movie in movies
                                 on ticket.MovieId equals movie.MovieId
                                 join genre in genres
                                 on movie.GenreId equals genre.GenreId
                                 join genre2 in genres
                                 on movie.SecondaryGenreId equals genre2.GenreId
                                 where ticket.MovieId == movieId
                                 select new SoldTicketData
                                 {
                                     TicketPrice = ticket.Price,
                                     Genre = genre.Name,
                                     SecondaryGenre = genre2.Name,
                                     Movie = movie.Name
                                 }).FirstOrDefault();

                Console.WriteLine(string.Format("Success, sold a {0} ticket to the {1}/{2} hit {3}", theTicket.TicketPrice, theTicket.Genre, theTicket.SecondaryGenre, theTicket.Movie));
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: Projection failed - " + ex.Message);
            }
        }

        private static void SelectTicketsUsingProjectionQueryingAgainstTheSameTableOnJoin2(int movieId)
        {
            // Select the item.
            Query<Ticket> tickets = new Query<Ticket>(db.Provider);
            Query<Movie> movies = new Query<Movie>(db.Provider);
            Query<Genre> genres = new Query<Genre>(db.Provider);
            Query<Genre> genres2 = new Query<Genre>(db.Provider);

            // Validate values.

            try
            {
                var theTicket = (from ticket in tickets
                                 join movie in movies
                                 on ticket.MovieId equals movie.MovieId
                                 join genre in genres
                                 on movie.GenreId equals genre.GenreId
                                 join genre2 in genres2
                                 on movie.SecondaryGenreId equals genre2.GenreId
                                 where ticket.MovieId == movieId
                                 select new SoldTicketData
                                 {
                                     TicketPrice = ticket.Price,
                                     Genre = genre.Name,
                                     SecondaryGenre = genre2.Name,
                                     Movie = movie.Name
                                 }).FirstOrDefault();

                Console.WriteLine(string.Format("Success, sold a {0} ticket to the {1}/{2} hit {3}", theTicket.TicketPrice, theTicket.Genre, theTicket.SecondaryGenre, theTicket.Movie));
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: Projection failed - " + ex.Message);
            }
        }


        private static void SelectUsingContains(IEnumerable<string> genreContains)
        {
            Console.WriteLine("Loading genres that contain {0}.", genreContains);

            Query<Genre> genres = new Query<Genre>(db.Provider);

            var query = from genre in genres
                        where genreContains.Contains(genre.Name)
                        select genre;

            BatchQuery batch = new BatchQuery(db.Provider);
            batch.Queue(query);
            ExecuteQuery(batch);
        }
        #endregion


        #region Batch Inserts
        private static void BatchInsertInto()
        {
            Console.WriteLine("Batch Inserting Genres using Insert.Into with unescaped values.");

            IList<Genre> genres = new List<Genre>
                {
                    new Genre { Name = "Genre's Third" },
                    new Genre { Name = "Genre's Fourth" }
                };

            BatchQuery bq = new BatchQuery(db.Provider);

            foreach (Genre genre in genres)
            {
                bq.Queue(db.Insert.Into<Genre>(g => g.Name).Values(genre.Name));
            }

            ExecuteQuery(bq);
        }

        private static void BatchToInsert()
        {
            Console.WriteLine("Batch Inserting Genres using Insert.Into with unescaped values.");

            IList<Genre> genres = new List<Genre>
                {
                    new Genre { Name = "Genre's First" },
                    new Genre { Name = "Genre's Second" }
                };

            BatchQuery bq = new BatchQuery(db.Provider);

            foreach (Genre genre in genres)
            {
                bq.Queue(genre.ToInsertQuery<Genre>(db.Provider));
            }

            ExecuteQuery(bq);
        }

        private static void BatchInsertOnNonReservedFieldNames(int movieIds)
        {
//            Console.WriteLine(
        }

        #endregion

        #region Helpers
        private static int ExecuteQuery(ISqlQuery queryToExecute)
        {
            int retVal = 0;

            try
            {
                retVal = queryToExecute.Execute();
                Console.WriteLine("Success");
            }
            catch (Exception ex)
            {
                Console.WriteLine("ERROR: " + ex.Message + "\r\n\t" + queryToExecute.BuildSqlStatement());
            }

            return retVal;
        }
        #endregion

    }
}
